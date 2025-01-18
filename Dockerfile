FROM alpine:3.20
ARG TARGETPLATFORM
RUN apk add --no-cache redis ca-certificates socat
COPY ./tmp/$TARGETPLATFORM /faktory

RUN mkdir -p /root/.faktory/db
RUN mkdir -p /.faktory/db
RUN mkdir -p /var/lib/faktory/db
RUN mkdir -p /etc/faktory

EXPOSE 7419 7420

RUN chgrp -R 0 /var/lib/faktory && \
    chmod -R g=u /var/lib/faktory && \
    chgrp -R 0 /.faktory/db && \
    chmod -R g=u /.faktory/db && \
    chgrp -R 0 /etc/faktory && \
    chmod -R g=u /etc/faktory

CMD ["/faktory", "-w", "0.0.0.0:7420", "-b", "0.0.0.0:7419"]
