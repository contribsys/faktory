FROM ubuntu:16.04 AS build

ARG ROCKSDB_VERSION
RUN apt-get update -y
RUN apt-get install -y liblz4-dev libbz2-dev libzstd-dev libsnappy-dev \
    libz-dev libjemalloc-dev build-essential git curl
RUN git clone --depth 1 --single-branch --branch v${ROCKSDB_VERSION} \
    https://github.com/facebook/rocksdb /rocksdb
WORKDIR /rocksdb
RUN PORTABLE=1 make static_lib
RUN strip -g librocksdb.a
ENV ROCKSDB_HOME /rocksdb

ARG GOLANG_VERSION
WORKDIR /usr/local
RUN curl https://storage.googleapis.com/golang/go${GOLANG_VERSION}.linux-amd64.tar.gz | tar xfz -
ENV PATH ${PATH}:/usr/local/go/bin

ARG FAKTORY_VERSION
ENV CGO_CFLAGS -I${ROCKSDB_HOME}/include
ENV CGO_LDFLAGS -L${ROCKSDB_HOME} -lrocksdb -llz4 -lbz2 -lzstd -lsnappy -lz -ljemalloc
ADD . /faktory
RUN cd /faktory && make prepare
RUN ln -s /faktory ${HOME}/go/src/github.com/contribsys/faktory
ENV PATH ${PATH}:/root/go/bin
WORKDIR /root/go/src/github.com/contribsys/faktory
RUN make test
RUN make build

FROM ubuntu:16.04
COPY --from=build /faktory/faktory /faktory/faktory-cli /
RUN apt-get update && apt-get install -y liblz4-dev libbz2-dev libzstd-dev \
    libsnappy-dev libz-dev libjemalloc-dev
RUN mkdir -p /root/.faktory/db

EXPOSE 7419 7420
ENTRYPOINT ["/faktory"]
