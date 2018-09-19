<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# TLS, Authorization, Authentication - Enabling encryption, authorization and authentication features
Pravega stores the critical data for the customers.The goal is to run Pravega as a central streaming storage for different parts of the organizations having different levels of access to the data. This feature is crucial to supprt multi tenancy.

Following key features are added as part of security implementation:
1. Pravega allows administrators to enable encryption for different communication channels using TLS.
2. Pravega provides role Based access control which can be availed by a variety of enterprises.
3. Pravega performs dynamic implementations of the Authorization/Authentication APIs. See [here](../auth/auth-plugin.md) for more details. Multiple implementations can co-exist and different plugins can be used by different users.
4. Multiple mechanisms are enabled by Pravega to the users for specifying _auth_ parameters to the client. See [here](../auth/client-auth.md) for more details. 
5. Components like Bookkeeper, Zookeeper etc., which are deployed with Pravega can be deployed securely with TLS.

[PDP-23](https://github.com/pravega/pravega/wiki/PDP-23:-Pravega-security----encryption-and-Role-Based-Access-Control) 
discusses various options for this design and anlayzes the pros and cons in detail. 
