FROM alpine:latest
RUN apk add --update ca-certificates
COPY bin/injector /
RUN chmod +x injector
ENTRYPOINT ["/injector"]