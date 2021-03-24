/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.client.security.auth;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.control.impl.Controller;
import io.pravega.shared.security.auth.AccessOperation;
import lombok.NonNull;

/**
 * Factory class for {@link DelegationTokenProvider} instances.
 */
public class DelegationTokenProviderFactory {

    /**
     * Creates a {@link DelegationTokenProvider} instance with empty token. Intended to be Used for testing only.
     *
     * @return a new {@link DelegationTokenProvider} instance
     */
    @VisibleForTesting
    public static DelegationTokenProvider createWithEmptyToken() {
        return new EmptyTokenProviderImpl();
    }

    /**
     * Creates a {@link DelegationTokenProvider} instance with null delegation token.
     *
     * @param controller the {@link Controller} client used for obtaining a delegation token from the Controller
     * @param scopeName the name of the scope tied to the segment, for which a delegation token is to be obtained
     * @param streamName the name of the stream tied to the segment, for which a delegation token is to be obtained
     * @param accessOperation the access operation to use when requesting a delegation token from the server
     * @return a new {@link DelegationTokenProvider} instance
     * @throws NullPointerException if {@code controller}, {@code scopeName} or {@code streamName} is null
     * @throws IllegalArgumentException if {@code scopeName} or {@code streamName} is empty
     */
    @VisibleForTesting
    public static DelegationTokenProvider create(Controller controller, String scopeName, String streamName,
                                                 AccessOperation accessOperation) {
        return create(null, controller, scopeName, streamName, accessOperation);
    }

    /**
     * Creates a {@link DelegationTokenProvider} instance with null delegation token.
     *
     * @param controller the {@link Controller} client used for obtaining a delegation token from the Controller
     * @param segment the {@link Segment}, for which a delegation token is to be obtained
     * @param accessOperation the access operation to use when requesting a delegation token from the server
     * @return a new {@link DelegationTokenProvider} instance
     * @throws NullPointerException if {@code controller} or {@code segment} is null
     */
    public static DelegationTokenProvider create(Controller controller, Segment segment, AccessOperation accessOperation) {
        return create(null, controller, segment, accessOperation);
    }

    /**
     * Creates a {@link DelegationTokenProvider} instance with the specified delegation token.
     *
     * @param delegationToken an existing delegation token to populate the {@link DelegationTokenProvider} instance with
     * @param controller  the {@link Controller} client used for obtaining a delegation token from the Controller
     * @param segment the {@link Segment}, for which a delegation token is to be obtained
     * @param accessOperation the access operation to use when requesting a delegation token from th server
     * @return a new {@link DelegationTokenProvider} instance
     * @throws NullPointerException if {@code controller} or {@code segment} is null
     */
    public static DelegationTokenProvider create(String delegationToken, @NonNull Controller controller,
                                                 @NonNull Segment segment, AccessOperation accessOperation) {
         return create(delegationToken, controller, segment.getScope(), segment.getStreamName(), accessOperation);
    }

    /**
     * Creates a {@link DelegationTokenProvider} instance of an appropriate type.
     *
     * @param delegationToken an existing delegation token to populate the {@link DelegationTokenProvider} instance with.
     * @param controller the {@link Controller} client used for obtaining a delegation token from the Controller if/when
     *                   the token expires or is nearing expiry
     * @param scopeName the name of the scope tied to the segment, for which a delegation token is to be obtained
     * @param streamName the name of the stream tied to the segment, for which a delegation token is to be obtained
     * @param accessOperation the access operation to use when requesting a delegation token from th server
     * @return a new {@link DelegationTokenProvider} instance
     */
    public static DelegationTokenProvider create(String delegationToken, Controller controller, String scopeName,
                                                 String streamName, AccessOperation accessOperation) {
        if (delegationToken == null) {
            return new JwtTokenProviderImpl(controller, scopeName, streamName, accessOperation);
        } else if (delegationToken.equals("")) {
            return new EmptyTokenProviderImpl();
        } else if (delegationToken.split("\\.").length == 3) {
            return new JwtTokenProviderImpl(delegationToken, controller, scopeName, streamName, accessOperation);
        } else {
            return new StringTokenProviderImpl(delegationToken);
        }
    }
}
