FROM alpine:3.11
RUN apk add --no-cache redis ca-certificates socat
COPY ./faktory /

RUN mkdir -p /root/.faktory/db
RUN mkdir -p /var/lib/faktory/db
RUN mkdir -p /etc/faktory

EXPOSE 7419 7420
CMD ["/faktory", "-w", "0.0.0.0:7420", "-b", "0.0.0.0:7419", "-e", "development"]
