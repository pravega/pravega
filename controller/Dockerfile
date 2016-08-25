FROM java:8
 
MAINTAINER Prabha Veerubhotla [Vijayalakshmi.Veerubhotla@emc.com]
 
COPY . /opt/controller/
 
WORKDIR /opt/controller/
 
RUN  ./gradlew jar
 
ENTRYPOINT [ "/opt/controller/gradlew",":controller:server:run" ] 

