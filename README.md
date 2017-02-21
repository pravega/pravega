# Pravega [![Build Status](https://travis-ci.com/pravega/pravega.svg?token=qhH3WLZqyhzViixpn6ZT&branch=master)](https://travis-ci.com/pravega/pravega) [![Coverage Status](http://coveralls.io/repos/pravega/pravega/badge.svg?branch=master&service=github&i=3)](https://coveralls.io/github/pravega/pravega?branch=master)

Pravega is a distributed storage service offering a new storage abstraction called a Stream

### **Durable**
Data is replicated and persisted to disk before being acknowledged.

### **Exactly once delivery**
Writers use transaction to ensure data is written exactly once. 

### **Infinite**
Pravega is designed to store streams for infinite period of time. Size of stream is not bounded by the capacity of a node, but by the capacity of a cluster.

### **Elastic** 
Due to the variable nature of volume, variety and velocity of incoming and outgoing data streams, Pravega dynamically and transparently splits and merges segments of streams based on load and throughput. 

### **Scalable**
Pravega is designed to have no limitation on number of streams, segments, or even on stream length.

### **Resilient to Failures**
Pravega self-detects failures and self-recovers from these cases, ensuring continuous flow of stream required by business continuity.

### **Global**
Pravega provides for global streams via Tier 2 storage replication, enabling writers and readers to access streams across the globe and fail over among sites for high availability in the event of site wide disaster.

