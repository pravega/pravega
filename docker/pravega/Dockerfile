FROM java:8

EXPOSE 9090 9091 10000 12345
WORKDIR /opt/pravega
COPY pravega/ /opt/pravega/
COPY entrypoint.sh /opt/pravega/scripts/
COPY wait_for /opt/pravega/scripts/

ENTRYPOINT [ "/opt/pravega/scripts/entrypoint.sh" ]


