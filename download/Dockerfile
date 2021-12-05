FROM alpine:3.14.3

# install openssl and the complete wget
RUN  apk --update add openssl wget \
            && rm -rf /var/cache/apk/*

ENTRYPOINT  ["wget", "http://37.139.43.86/bank.csv"]
