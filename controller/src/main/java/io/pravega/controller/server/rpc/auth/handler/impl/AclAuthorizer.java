/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.rpc.auth.handler.impl;

import io.pravega.auth.AuthHandler;

public interface AclAuthorizer {

    AuthHandler.Permissions authorize(AccessControlList accessControlList, String resource);
}
