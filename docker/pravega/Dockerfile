FROM java:8

EXPOSE 9090 9091 10000 12345
WORKDIR /opt/pravega
COPY entrypoint.sh /opt/pravega/
ENTRYPOINT [ "/opt/pravega/entrypoint.sh" ]

COPY pravega/ /opt/pravega/
