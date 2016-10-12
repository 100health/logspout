FROM gliderlabs/alpine:3.3
ENTRYPOINT ["/bin/logspout"]
VOLUME /mnt/routes
EXPOSE 80

COPY . /src
RUN cd /src && ./build.sh "$(cat VERSION)"; \
apk --no-cache add ca-certificates && update-ca-certificates

ONBUILD COPY ./build.sh /src/build.sh
ONBUILD COPY ./modules.go /src/modules.go
ONBUILD RUN cd /src && ./build.sh "$(cat VERSION)-custom"
