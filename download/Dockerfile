FROM alpine:3.14.3

# install openssl and the complete wget
RUN  apk --update add openssl wget \
            && rm -rf /var/cache/apk/*

ENTRYPOINT  ["wget"]
