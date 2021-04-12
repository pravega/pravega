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
import com.google.common.base.Preconditions;
import io.pravega.client.control.impl.Controller;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.util.ConfigurationOptionsExtractor;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import io.pravega.shared.security.auth.AccessOperation;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.common.security.JwtUtils.extractExpirationTime;

/**
 * Provides JWT-based delegation tokens.
 */
@Slf4j
public class JwtTokenProviderImpl implements DelegationTokenProvider {

    /**
     * Represents the default threshold (in seconds) for triggering delegation token refresh.
     */
    @VisibleForTesting
    static final int DEFAULT_REFRESH_THRESHOLD_SECONDS = 5;

    private static final String REFRESH_THRESHOLD_SYSTEM_PROPERTY = "pravega.client.auth.token-refresh.threshold";

    private static final String REFRESH_THRESHOLD_ENV_VARIABLE = "pravega_client_auth_token-refresh.threshold";

    /**
     * Represents the threshold (in seconds) for triggering delegation token refresh.
     */
    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final int refreshThresholdInSeconds;

    /**
     * The Controller gRPC client used for interacting with the server.
     */
    private final Controller controllerClient;

    private final String scopeName;

    private final String streamName;

    private final AccessOperation accessOperation;

    private final AtomicReference<DelegationToken> delegationToken = new AtomicReference<>();

    private final AtomicBoolean tokenExpirySignal = new AtomicBoolean(false);

    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final AtomicReference<CompletableFuture<Void>> tokenRefreshFuture = new AtomicReference<>();

    JwtTokenProviderImpl(Controller controllerClient, String scopeName, String streamName, AccessOperation accessOperation) {
        this(controllerClient, scopeName, streamName, ConfigurationOptionsExtractor.extractInt(
                REFRESH_THRESHOLD_SYSTEM_PROPERTY, REFRESH_THRESHOLD_ENV_VARIABLE, DEFAULT_REFRESH_THRESHOLD_SECONDS),
                accessOperation);
    }

    private JwtTokenProviderImpl(Controller controllerClient, String scopeName, String streamName,
                                 int refreshThresholdInSeconds, AccessOperation accessOperation) {
        Exceptions.checkNotNullOrEmpty(scopeName, "scopeName");
        Preconditions.checkNotNull(controllerClient, "controllerClient is null");
        Exceptions.checkNotNullOrEmpty(streamName, "streamName");
        Preconditions.checkNotNull(accessOperation, "accessOperation");

        this.scopeName = scopeName;
        this.streamName = streamName;
        this.controllerClient = controllerClient;
        this.refreshThresholdInSeconds = refreshThresholdInSeconds;
        this.accessOperation = accessOperation;
    }

    /**
     * Constructs a new object with the specified {delegationToken}, {@code controllerClient}, {@code scopeName} and
     * {@code streamName}.
     *
     * @param token the initial delegation token
     * @param controllerClient the {@link Controller} client used for obtaining a delegation token from the Controller
     * @param scopeName the name of the scope tied to the segment, for which a delegation token is to be obtained
     * @param streamName the name of the stream tied to the segment, for which a delegation token is to be obtained
     */
    JwtTokenProviderImpl(String token, Controller controllerClient, String scopeName,
                                String streamName, AccessOperation accessOperation) {
        this(token, controllerClient, scopeName, streamName, ConfigurationOptionsExtractor.extractInt(
                "pravega.client.auth.token-refresh.threshold",
                "pravega_client_auth_token-refresh.threshold",
                DEFAULT_REFRESH_THRESHOLD_SECONDS), accessOperation);
    }

    /**
     * Constructs a new object with the specified {delegationToken}, {@code controllerClient}, {@code scopeName} and
     * {@code streamName}.
     *
     * @param token the initial delegation token
     * @param controllerClient the {@link Controller} client used for obtaining a delegation token from the Controller
     * @param scopeName the name of the scope tied to the segment, for which a delegation token is to be obtained
     * @param streamName the name of the stream tied to the segment, for which a delegation token is to be obtained
     * @param refreshThresholdInSeconds the time in seconds before expiry that should trigger a token refresh
     */
    JwtTokenProviderImpl(String token, Controller controllerClient, String scopeName, String streamName,
                                    int refreshThresholdInSeconds, AccessOperation accessOperation) {
        Exceptions.checkNotNullOrEmpty(token, "delegationToken");
        Exceptions.checkNotNullOrEmpty(scopeName, "scopeName");
        Preconditions.checkNotNull(controllerClient, "controllerClient is null");
        Exceptions.checkNotNullOrEmpty(streamName, "streamName");

        Long expTime = extractExpirationTime(token);
        delegationToken.set(new DelegationToken(token, expTime));
        this.scopeName = scopeName;
        this.streamName = streamName;
        this.controllerClient = controllerClient;
        this.refreshThresholdInSeconds = refreshThresholdInSeconds;
        this.accessOperation = accessOperation;
    }

    /**
     * Returns the delegation token. It returns existing delegation token if it is not close to expiry. If the token
     * is close to expiry, it obtains a new delegation token and returns that one instead.
     *
     * @return a CompletableFuture that, when completed, will return the delegation token JWT compact value
     */
    @Override
    public CompletableFuture<String> retrieveToken() {
        DelegationToken currentToken = this.delegationToken.get();
        final CompletableFuture<String> result;
        if (currentToken == null) {
            result = this.refreshToken();
        } else if (currentToken.getExpiryTime() == null) {
            result = CompletableFuture.completedFuture(currentToken.getValue());
        } else if (this.tokenExpirySignal.get()) {
            log.debug("Token was signaled as expired for scope/stream {}/{}", this.scopeName, this.streamName);
            result = refreshToken();
            this.tokenExpirySignal.compareAndSet(true, false);
        } else if (isTokenNearingExpiry(currentToken)) {
            log.debug("Token is nearing expiry for scope/stream {}/{}", this.scopeName, this.streamName);
            result = refreshToken();
        } else {
            result = CompletableFuture.completedFuture(currentToken.getValue());
        }
        return result;
    }

    @Override
    public boolean populateToken(String token) {
        DelegationToken currentToken = this.delegationToken.get();
        if (token == null || (currentToken != null && currentToken.getValue().equals(""))) {
            return false;
        } else {
            return this.delegationToken.compareAndSet(currentToken, new DelegationToken(token, extractExpirationTime(token)));
        }
    }

    @Override
    public void signalTokenExpired() {
        this.tokenExpirySignal.compareAndSet(false, true);
    }

    private boolean isTokenNearingExpiry(DelegationToken token) {
        Long currentTokenExpirationTime = token.getExpiryTime();

        // currentTokenExpirationTime can be null if the server returns a null delegation token
        return currentTokenExpirationTime != null && isWithinRefreshThreshold(currentTokenExpirationTime);
    }

    private boolean isWithinRefreshThreshold(Long expirationTime) {
        assert expirationTime != null;
        return isWithinRefreshThreshold(Instant.now(), Instant.ofEpochSecond(expirationTime));
    }

    @VisibleForTesting
    boolean isWithinRefreshThreshold(Instant currentInstant, Instant expiration) {
        return currentInstant.plusSeconds(refreshThresholdInSeconds).getEpochSecond() >= expiration.getEpochSecond();
    }

    @VisibleForTesting
    CompletableFuture<String> refreshToken() {
        long traceEnterId = LoggerHelpers.traceEnter(log, "refreshToken", this.scopeName, this.streamName);
        CompletableFuture<Void> currentRefreshFuture = tokenRefreshFuture.get();
        if (currentRefreshFuture == null) {
            log.debug("Initiating token refresh for scope {} and stream {}", this.scopeName, this.streamName);
            currentRefreshFuture = this.recreateToken();
            this.tokenRefreshFuture.compareAndSet(null, currentRefreshFuture);
        } else {
            log.debug("Token is already under refresh for scope {} and stream {}", this.scopeName, this.streamName);
        }

        final CompletableFuture<Void> handleToCurrentRefreshFuture  = currentRefreshFuture;
        return currentRefreshFuture.handle((v, ex) -> {
            this.tokenRefreshFuture.compareAndSet(handleToCurrentRefreshFuture, null);
            LoggerHelpers.traceLeave(log, "refreshToken", traceEnterId, this.scopeName, this.streamName);
            if (ex != null) {
                log.warn("Encountered an exception in when refreshing token for scope {} and stream {}",
                        this.scopeName, this.streamName, Exceptions.unwrap(ex));
                throw ex instanceof CompletionException ? (CompletionException) ex : new CompletionException(ex);
            } else {
                return delegationToken.get().getValue();
            }
        });
    }

    private CompletableFuture<Void> recreateToken() {
        return controllerClient.getOrRefreshDelegationTokenFor(scopeName, streamName, accessOperation)
                .thenAccept(token -> this.delegationToken.set(new DelegationToken(token, extractExpirationTime(token))));
    }
}
