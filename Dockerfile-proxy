FROM golang:1.12.1 as builder

COPY . /go/src/seata.io/server
WORKDIR /go/src/seata.io/server

RUN make seata-go-proxy

FROM alpine:latest

COPY --from=builder /go/src/seata.io/server/dist/seata-go-proxy /usr/local/bin/seata-go-proxy

RUN mkdir -p /var/seata-go-proxy/
RUN mkdir -p /var/lib/seata-go-proxy/

# Alpine Linux doesn't use pam, which means that there is no /etc/nsswitch.conf,
# but Golang relies on /etc/nsswitch.conf to check the order of DNS resolving
# (see https://github.com/golang/go/commit/9dee7771f561cf6aee081c0af6658cc81fac3918)
# To fix this we just create /etc/nsswitch.conf and add the following line:
RUN echo 'hosts: files mdns4_minimal [NOTFOUND=return] dns mdns4' >> /etc/nsswitch.conf

# Define default command.
ENTRYPOINT ["/usr/local/bin/seata-go-proxy"]
