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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.pravega.auth.AuthConstants;
import io.pravega.auth.AuthException;
import io.pravega.auth.AuthHandler;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.security.Principal;

/**
 * Intercepts gRPC requests and sets up Auth context.
 */
@Slf4j
public class AuthInterceptor implements ServerInterceptor {

    public static final String PRINCIPAL_KEY_NAME = "PravegaContext";
    public static final String INTERCEPTOR_KEY_NAME = "InterceptorContext";

    /**
     * Represents the key used for indexing the {@link Principal} object stored in the current
     * {@link Context}.
     */
    static final Context.Key<Principal> PRINCIPAL_OBJECT_KEY = Context.key(PRINCIPAL_KEY_NAME);

    /**
     * Represents the key used for indexing an object instance of this class stored in the current
     * {@link Context}.
     */
    static final Context.Key<AuthInterceptor> AUTH_INTERCEPTOR_OBJECT_KEY = Context.key(INTERCEPTOR_KEY_NAME);

    @Getter
    private final AuthHandler handler;

    @VisibleForTesting
    public AuthInterceptor(AuthHandler handler) {
        Preconditions.checkNotNull(handler, "handler can not be null");
        this.handler = handler;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

        Context context = Context.current();

        // The authorization header has the credentials (e.g., username and password for Basic Authentication).
        // The form of the header is: <Method> <Token> (CustomMethod static-token, or Basic XYZ...., for example)
        String credentials = headers.get(Metadata.Key.of(AuthConstants.AUTHORIZATION, Metadata.ASCII_STRING_MARSHALLER));

        if (!Strings.isNullOrEmpty(credentials)) {
            String[] parts = credentials.split("\\s+", 2);
            if (parts.length == 2) {
                String method = parts[0];
                String token = parts[1];
                if (!Strings.isNullOrEmpty(method)) {
                    if (method.equals(handler.getHandlerName())) {
                        log.debug("Handler [{}] successfully matched auth method [{}]", handler, method);
                        Principal principal;
                        try {
                            if ((principal = handler.authenticate(token)) == null) {
                                log.warn("Handler for method [{}] returned a null Principal upon authentication for the"
                                        + "given token", method);
                                call.close(Status.fromCode(Status.Code.UNAUTHENTICATED), headers);
                                return null;
                            }
                        } catch (AuthException e) {
                            log.warn("Authentication failed", e);
                            call.close(Status.fromCode(Status.Code.UNAUTHENTICATED), headers);
                            return null;
                        }
                        // Creates a new Context with the given key/value pairs.
                        context = context.withValues(PRINCIPAL_OBJECT_KEY, principal, AUTH_INTERCEPTOR_OBJECT_KEY, this);
                    }
                } else {
                    log.debug("Credentials are present, but method [{}] is null or empty", method);
                }
            }
        }

        // reaching this point means that the handler wasn't applicable to this request.
        return Contexts.interceptCall(context, call, headers, next);
    }
}