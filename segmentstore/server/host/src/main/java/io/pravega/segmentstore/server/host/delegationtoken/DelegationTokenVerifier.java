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

/**
 * This interface represents the code on segment store side that verifies the delegation token.
 */
public interface DelegationTokenVerifier {
    /**
     * Verifies whether the token represents access to a given resource to the level expected.
     * @param resource       The resource for which access is to be verified.
     * @param token          The delegation token.
     * @param expectedLevel  Maximum expected access to the given resoure after this verification.
     * @return               true if the delegation token allows access to the resource for the expected level.
     */
    boolean verifyToken(String resource, String token, PravegaAuthHandler.PravegaAccessControlEnum expectedLevel);
}
