FROM java:8
 
MAINTAINER Prabha Veerubhotla [Vijayalakshmi.Veerubhotla@emc.com]
 
COPY server/build/install/server/  /opt/controller/
 
WORKDIR /opt/controller/
  
ENTRYPOINT [ "/opt/controller/bin/server" ] 

