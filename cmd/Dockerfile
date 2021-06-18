# build stage
FROM golang:1.16-alpine3.13 AS build-env
RUN apk --no-cache add build-base git mercurial gcc
ADD . /source
RUN cd /source && go build -o injector cmd/injector.go

FROM alpine:3.13
WORKDIR /app
RUN apk add --update ca-certificates
COPY --from=build-env /source/injector /app/
ENTRYPOINT ./injector