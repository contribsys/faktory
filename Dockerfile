ARG GOLANG_VERSION
FROM golang:${GOLANG_VERSION}-alpine3.8 AS build

RUN apk add --no-cache build-base git ca-certificates redis

ENV GOPATH /root/go
ENV PATH ${PATH}:/root/go/bin

RUN mkdir -p /root/go/src/github.com/contribsys
ADD . /root/go/src/github.com/contribsys/faktory
WORKDIR /root/go/src/github.com/contribsys/faktory
RUN make prepare && make test && make build
#RUN make prepare && make build

FROM alpine:3.7
#RUN apk add --no-cache redis bash
RUN apk add --no-cache redis
COPY --from=build /root/go/src/github.com/contribsys/faktory/faktory \
                  /

RUN mkdir -p /root/.faktory/db
RUN mkdir -p /var/lib/faktory/db
RUN mkdir -p /etc/faktory

EXPOSE 7419 7420
CMD ["/faktory", "-w", "0.0.0.0:7420", "-b", "0.0.0.0:7419", "-e", "development"]
