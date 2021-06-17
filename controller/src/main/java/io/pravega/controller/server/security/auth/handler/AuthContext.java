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
package io.pravega.controller.server.security.auth.handler;

import io.pravega.auth.AuthHandler;
import lombok.Getter;

import java.security.Principal;

/**
 * A gateway to objects held in gRPC {@link io.grpc.Context} by the {@link AuthInterceptor}, for the current gRPC
 * request.
 */
public class AuthContext {

    @Getter
    private final Principal principal;

    @Getter
    private final AuthHandler authHandler;

    /**
     * Constructs an object.
     *
     * @param principal the principal representing the subject. Null is allowed.
     * @param authHandler the authHandler. Null is allowed.
     */
    private AuthContext(Principal principal, AuthHandler authHandler) {
        this.principal = principal;
        this.authHandler = authHandler;
    }

    /**
     * Initiates a new instance using the current {@link io.grpc.Context}.
     *
     * @return an instance containing {@link Principal} and {@link AuthHandler} from the current {@link io.grpc.Context}
     */
    public static AuthContext current() {
        // Obtains the Principal object stored in the current gRPC Context. The 'get()' method gets the value from the
        // Context associated with the current gRPC scope, so the value changes for each gRPC request/scope.
        Principal principal = AuthInterceptor.PRINCIPAL_OBJECT_KEY.get();

        // Obtains the AuthInterceptor object stored in the current gRPC Context. The 'get()' method gets the value
        // from the Context associated with the current gRPC scope, so the value changes for each gRPC request/scope.
        AuthInterceptor serverAuthInterceptor = AuthInterceptor.AUTH_INTERCEPTOR_OBJECT_KEY.get();

        AuthHandler authHandler = null;
        if (serverAuthInterceptor != null) {
            authHandler = serverAuthInterceptor.getHandler();
        }
        return new AuthContext(principal, authHandler);
    }
}
