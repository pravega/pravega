# Pravega [![Build Status](https://travis-ci.com/pravega/pravega.svg?token=qhH3WLZqyhzViixpn6ZT&branch=master)](https://travis-ci.com/pravega/pravega) [![codecov](https://codecov.io/gh/pravega/pravega/branch/master/graph/badge.svg?token=6xOvaR0sIa)](https://codecov.io/gh/pravega/pravega)

Pravega is an open source distributed storage service implementing streams. A
stream is the foundation for reliable streaming systems: a *high-performance,
durable, elastic, and infinite append-only byte stream with strict ordering and
consistency*.

### Features 

-   Auto Scaling - Automatically scale individual data streams across storage
    and processing resources to accommodate dynamically changing data ingestion
    rate.

-   Infinite Retention - Ingest, process and retain events as streams for ever.
    Use same paradigm to access both real-time and historical events stored in
    Pravega.

-   Durability - Don't comprise between performance, durability and consistency.
    Pravega replicates and persists the ingested event before acknowledging.

-   Exactly-Once Semantics - Ensure that each message is delivered and processed
    exactly once, despite failures on both client, server and network sides.

-   Transaction Support - Use transactional data ingestion to achieve guarantee
    when writing a series of related events to a stream at once.

-   Multi-Protocol Access - With multi-protocol capability, Access and process
    your streams not only via Pravega Streaming API, but via HDFS protocol.

-   Write Efficiency - Pravega shrinks write latency to milliseconds, and
    seamlessly scales to handle high throughput reads and writes from thousands
    of concurrent clients.

-   Fault Tolerant - Don't aim for availability only. Pravega
    automatically detects failures and recovers from them ensuring the
    continuous flow of data required for business continuity.

### Create stream

```
Write code snippet
```

### Write events to a steam 

```
Read/write  
```

### Read events from a stream 

```
code snipper 
```

Building Pravega from Source
----------------------------

To build Pravega 

Deployment Options 
-------------------

1.  Single

2.  Manual Install

3.  Docker Based

Support
-------

Don’t hesitate to ask! Contact the developers and community on the mailing lists
if you need any help. Open an issue if you found a bug on [Github
Issues](https://github.com/pravega/pravega/issues)

Documentation
-------------

The Pravega documentation of is hosted on the website:
<http://pravega.io/docs> or in the
[docs/](https://github.com/pravega/pravega/tree/master/docs) directory of the
source code.

Contribute
----------

Become one of early contributors! We thrive to build an welcoming and open
community for anyone who wants to use the system or contribute to it.
[Here](https://github.com/pravega/pravega/wiki/Contributing) we describe how to
contribute to Pravega!

About
-----

Pravega is 100% open source and community-driven. All components are available
under [Apache 2 License](https://www.apache.org/licenses/LICENSE-2.0.html) on
GitHub.
