
FROM gcr.io/cloud-builders/go as builder

ENV CGO_ENABLED 0
ENV PKG /root/go/src/github.com/eoscanada/pitreos
RUN mkdir -p $PKG
COPY . $PKG
RUN cd $PKG \
    && go get -v -t . \
    && go test -v \
    && go build -v -o /pitreos


FROM ubuntu:18.04

COPY --from=builder /pitreos /app/pitreos

RUN apt update && apt-get install -y ca-certificates
