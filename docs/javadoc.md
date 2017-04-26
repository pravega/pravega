# Java API Reference

### **Clients**
A Writer is a client that creates Events and publishes them into Streams.
A Reader is a client that Consumes events from Streams.
We provide a Java library, which implements a convenient API for Writer and Reader applications to use.  The client library encapsulates the wire protocol that is used to convey requests and responses between Pravega Clients and the Pravega service.

[Writer and Reader API](javadoc/clients/index.html)

### **Common**
Both client, controller and server libraries use common set of functionalities, which are packaged under 
[Common API](javadoc/common/index.html)

### **Controller**
The Stream Controller is a stateless process that manages the control plane of the system,
keeping information about which Streams exist in the system,
how many Segments there are per Stream etc,
how and when these streams need to be scaled up or down etc..
The Stream Controller provides an API to create Streams and other control plane operations, and can be found here [Controller API](javadoc/controller/index.html)

### **Segment Store**
Pravega Segment Store is the data plane of the system, storing and serving actual stream data. To to write/read events to a stream, clients are redirected by Controller to Pravega server, which contains the requested stream segments.
Once connection has been established with the right Pravega Server, clients use following set of APIs to read/write events [Pravega Server API](javadoc/segmentstore/index.html)
