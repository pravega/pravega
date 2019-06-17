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

import io.pravega.auth.AuthHandler;

/**
 * Default implementation of DelegegationTokenVerifier which asserts all the tokens as valid.
 * This is used when no verifier is configured.
 */
public class PassingTokenVerifier implements DelegationTokenVerifier {
    @Override
    public void verifyToken(String resource, String token, AuthHandler.Permissions expectedLevel) {}
}