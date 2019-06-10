FROM golang:1.12.1 as builder

ARG RELEASE=dev
ARG TARGET=seata

COPY . /go/src/seata.io/server
WORKDIR /go/src/seata.io/server

RUN make ${TARGET} 'release_version=${RELEASE}'

FROM alpine:latest

COPY --from=builder /go/src/seata.io/server/dist/${TARGET} /usr/local/bin/${TARGET}
COPY dist/ui/dist /app/seata/ui

RUN mkdir -p /var/${TARGET}/
RUN mkdir -p /var/lib/${TARGET}/

# Alpine Linux doesn't use pam, which means that there is no /etc/nsswitch.conf,
# but Golang relies on /etc/nsswitch.conf to check the order of DNS resolving
# (see https://github.com/golang/go/commit/9dee7771f561cf6aee081c0af6658cc81fac3918)
# To fix this we just create /etc/nsswitch.conf and add the following line:
RUN echo 'hosts: files mdns4_minimal [NOTFOUND=return] dns mdns4' >> /etc/nsswitch.conf

# Define default command.
ENTRYPOINT ["/usr/local/bin/#TARGET#"]
