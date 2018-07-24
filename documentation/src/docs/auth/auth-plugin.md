<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Implementation of Pravega Authentication/Authorization plugin

This guide describes the authentication/authorization plugin model for Pravega.

## Pravega auth interface
The custom implementation is expected to implement the [AuthHandler](https://github.com/pravega/pravega/blob/master/shared/authplugin/src/main/java/io/pravega/auth/AuthHandler.java) interface.

## Dynamically loading auth implementations

Administrator users can implement their own authorization/authentication plugins. Multiple such plugins can exist together.
The plugin implementation follows the [Java Service Loader](https://docs.oracle.com/javase/7/docs/api/java/util/ServiceLoader.html) approach.
Pravega Controller expects the custom implementation Jars to be dropped in to the CLASSPATH.

The custom implementation is expected to implement the [AuthHandler](https://github.com/pravega/pravega/blob/master/shared/authplugin/src/main/java/io/pravega/auth/AuthHandler.java) interface.


