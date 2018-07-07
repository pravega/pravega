<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
### grpc client auth interface
In case more than one plugin exists, a client selects its auth handler by setting a grpc header with a name "method". 
This is done by implementing [PravegaCredentials](https://github.com/pravega/pravega/blob/master/client/src/main/java/io/pravega/client/stream/impl/Credentials.java) interface.
 This is passed through the [ClientConfig] () object to the 

Similar 