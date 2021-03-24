<!--
Copyright Pravega Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
# Implementation of Pravega Authentication/Authorization Plugin

This guide describes in detail the Authentication/Authorization plugin model for Pravega.

## Pravega _auth_ interface
The custom implementation performs the implementation of the [AuthHandler](https://github.com/pravega/pravega/blob/master/shared/authplugin/src/main/java/io/pravega/auth/AuthHandler.java) interface.

## Dynamic loading of _auth_ implementations

Administrators and users are allowed to implement their own Authorization/Authentication plugins. Multiple plugins of such kind can exist together.
The implementation of plugin follows the [Java Service Loader](https://docs.oracle.com/javase/7/docs/api/java/util/ServiceLoader.html) approach.
The required Jars for the custom implementation needs to be located in the **CLASSPATH** to enable the access for Pravega Controller for implementation.

**Note:** The custom implementation performs the implementation of the [AuthHandler](https://github.com/pravega/pravega/blob/master/shared/authplugin/src/main/java/io/pravega/auth/AuthHandler.java) interface.


