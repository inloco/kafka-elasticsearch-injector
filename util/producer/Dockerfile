FROM alpine:latest
RUN apk add --update ca-certificates
COPY bin/producer /
RUN chmod +x producer
ENTRYPOINT ["/producer"]