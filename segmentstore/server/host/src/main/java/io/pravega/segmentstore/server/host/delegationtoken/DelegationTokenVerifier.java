/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.delegationtoken;

import io.pravega.client.auth.PravegaAuthHandler;

public interface DelegationTokenVerifier {
    boolean verifyToken(String resource, String token, PravegaAuthHandler.PravegaAccessControlEnum expectedLevel);
}
