package syslog

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"log/syslog"
	"net"
	"os"
	"strconv"
	"syscall"
	"text/template"
	"time"

	"github.com/gliderlabs/logspout/router"
	"github.com/DataDog/datadog-go/statsd"
)

const defaultRetryCount = 10

var hostname string
var retryCount uint
var datadogClient *statsd.Client

func incr_metric(metric string) error {
	if datadogClient == nil {
		log.Printf("datadog: client is nil\n")
		return nil
	}
	return datadogClient.Incr(metric, nil, 1)
}

func init() {
	hostname, _ = os.Hostname()
	router.AdapterFactories.Register(NewSyslogAdapter, "syslog")

	if count, err := strconv.Atoi(getopt("RETRY_COUNT", strconv.Itoa(defaultRetryCount))); err != nil {
		retryCount = defaultRetryCount
	} else {
		retryCount = uint(count)
	}

	datadogStatsdServer := getopt("DATADOG_STATSD_SERVER", "")
	if datadogStatsdServer == "" {
		fmt.Println("# datadog: disabled metric reporting (no DATADOG_STATSD_SERVER set)")
	} else {
		client, datadogErr := statsd.New(datadogStatsdServer)
		if datadogErr == nil {
			log.Printf("# datadog: connected to statsd server: %s\n", datadogStatsdServer)
		} else {
			log.Printf("# datadog: error connecting to statsd server %s: %s\n", datadogStatsdServer, datadogErr)
		}
		client.Namespace = "logspout."
		datadogClient = client
	}
}

func getopt(name, dfault string) string {
	value := os.Getenv(name)
	if value == "" {
		value = dfault
	}
	return value
}

func NewSyslogAdapter(route *router.Route) (router.LogAdapter, error) {
	transport, found := router.AdapterTransports.Lookup(route.AdapterTransport("udp"))
	if !found {
		return nil, errors.New("bad transport: " + route.Adapter)
	}

	dogErr := incr_metric("connection_attempts")
	if dogErr != nil {
		log.Printf("datadog: Error incrementing connection_attempts metric: %s\n", dogErr)
	}

	conn, err := transport.Dial(route.Address, route.Options)
	if err != nil {
		return nil, err
	}

	format := getopt("SYSLOG_FORMAT", "rfc5424")
	priority := getopt("SYSLOG_PRIORITY", "{{.Priority}}")
	hostname := getopt("SYSLOG_HOSTNAME", "{{.Container.Config.Hostname}}")
	pid := getopt("SYSLOG_PID", "{{.Container.State.Pid}}")
	tag := getopt("SYSLOG_TAG", "{{.ContainerName}}"+route.Options["append_tag"])
	structuredData := getopt("SYSLOG_STRUCTURED_DATA", "")
	if route.Options["structured_data"] != "" {
		structuredData = route.Options["structured_data"]
	}
	data := getopt("SYSLOG_DATA", "{{.Data}}")
	timestamp := getopt("SYSLOG_TIMESTAMP", "{{.Timestamp}}")

	if structuredData == "" {
		structuredData = "-"
	} else {
		structuredData = fmt.Sprintf("[%s]", structuredData)
	}

	var tmplStr string
	switch format {
	case "rfc5424":
		tmplStr = fmt.Sprintf("<%s>1 %s %s %s %s - %s %s\n",
			priority, timestamp, hostname, tag, pid, structuredData, data)
	case "rfc3164":
		tmplStr = fmt.Sprintf("<%s>%s %s %s[%s]: %s\n",
			priority, timestamp, hostname, tag, pid, data)
	default:
		return nil, errors.New("unsupported syslog format: " + format)
	}
	tmpl, err := template.New("syslog").Parse(tmplStr)
	if err != nil {
		return nil, err
	}
	return &SyslogAdapter{
		route:     route,
		conn:      conn,
		tmpl:      tmpl,
		transport: transport,
	}, nil
}

type SyslogAdapter struct {
	conn      net.Conn
	route     *router.Route
	tmpl      *template.Template
	transport router.AdapterTransport
}

func (a *SyslogAdapter) Stream(logstream chan *router.Message) {
	for message := range logstream {
		m := &SyslogMessage{message}
		buf, err := m.Render(a.tmpl)
		if err != nil {
			log.Println("syslog:", err)
			return
		}
		_, err = a.conn.Write(buf)
		if err != nil {
			dogErr := incr_metric("send_errors")
			if dogErr != nil {
				log.Printf("datadog: Error incrementing send_errors metric: %s\n", dogErr)
			}
			log.Println("syslog:", err)
			switch a.conn.(type) {
			case *net.UDPConn:
				continue
			default:
				err = a.retry(buf, err)
				if err != nil {
					log.Println("syslog:", err)
					return
				}
			}
		}
	}
}

func (a *SyslogAdapter) retry(buf []byte, err error) error {
	if opError, ok := err.(*net.OpError); ok {
		if (opError.Temporary() && opError.Err != syscall.ECONNRESET) || opError.Timeout() {
			log.Println("retrying...")
			retryErr := a.retryTemporary(buf)
			if retryErr == nil {
				return nil
			}
		}
	}

	return a.reconnect()
}

func (a *SyslogAdapter) retryTemporary(buf []byte) error {
	log.Printf("syslog: retrying tcp up to %v times\n", retryCount)
	err := retryExp(func() error {

		dogErr := incr_metric("send_attempts")
		if dogErr != nil {
			log.Printf("datadog: Error incrementing send_attempts metric: %s\n", dogErr)
		}
		log.Println("syslog: retrying...")
		_, err := a.conn.Write(buf)
		if err == nil {
			log.Println("syslog: retry successful")
			return nil
		}

		return err
	}, retryCount)

	if err != nil {
		log.Println("syslog: retry failed")
		dogErr := incr_metric("send_failures")
		if dogErr != nil {
			log.Printf("datadog: Error incrementing send_failures metric: %s\n", dogErr)
		}
		return err
	}

	return nil
}

func (a *SyslogAdapter) reconnect() error {
	log.Printf("syslog: reconnecting up to %v times\n", retryCount)
	err := retryExp(func() error {

		dogErr := incr_metric("connection_attempts")
		if dogErr != nil {
			log.Printf("datadog: Error incrementing connection_attempts metric: %s\n", dogErr)
		}

		log.Println("syslog: reconnecting...")

		conn, err := a.transport.Dial(a.route.Address, a.route.Options)
		if err != nil {
			return err
		}

		a.conn = conn
		return nil
	}, retryCount)

	if err != nil {
		log.Println("syslog: reconnect failed. exiting...")
		dogErr := incr_metric("connection_failures")
		if dogErr != nil {
			log.Printf("datadog: Error incrementing connection_failures metric: %s\n", dogErr)
		}
		os.Exit(3)
	} else {
		log.Println("syslog: reconnect succeeded.")
	}

	return nil
}

func retryExp(fun func() error, tries uint) error {
	try := uint(0)
	for {
		err := fun()
		if err == nil {
			return nil
		}

		try++
		if try > tries {
			return err
		}

		time.Sleep((1 << try) * 10 * time.Millisecond)
	}
}

type SyslogMessage struct {
	*router.Message
}

func (m *SyslogMessage) Render(tmpl *template.Template) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := tmpl.Execute(buf, m)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (m *SyslogMessage) Priority() syslog.Priority {
	switch m.Message.Source {
	case "stdout":
		return syslog.LOG_USER | syslog.LOG_INFO
	case "stderr":
		return syslog.LOG_USER | syslog.LOG_ERR
	default:
		return syslog.LOG_DAEMON | syslog.LOG_INFO
	}
}

func (m *SyslogMessage) Hostname() string {
	return hostname
}

func (m *SyslogMessage) Timestamp() string {
	return m.Message.Time.Format(time.RFC3339)
}

func (m *SyslogMessage) ContainerName() string {
	return m.Message.Container.Name[1:]
}

func (m *SyslogMessage) ContainerShortID() string {
	return m.Message.Container.ID[0:12]
}