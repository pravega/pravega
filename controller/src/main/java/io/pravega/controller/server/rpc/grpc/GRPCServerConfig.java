/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.rpc.grpc;

import io.pravega.auth.ServerConfig;
import java.util.Optional;

/**
 * Configuration of controller gRPC server.
 */
public interface GRPCServerConfig extends ServerConfig {
    /**
     * Fetches the port on which controller gRPC server listens.
     *
     * @return the port on which controller gRPC server listens.
     */
    int getPort();

    /**
     * Fetches the RPC address which has to be registered with the cluster used for external access.
     *
     * @return The RPC address which has to be registered with the cluster used for external access.
     */
    Optional<String> getPublishedRPCHost();

    /**
     * Fetches the RPC port which has to be registered with the cluster used for external access.
     *
     * @return The RPC port which has to be registered with the cluster used for external access.
     */
    Optional<Integer> getPublishedRPCPort();

    /**
     * Fetches the settings which indicates whether authorization is enabled.
     *
     * @return Whether this deployment has auth enabled.
     */
    boolean isAuthorizationEnabled();

    /**
     * Fetches the location of password file for the users.
     * This uses UNIX password file syntax. It is used for default implementation of authorization
     * and authentication.
     *
     * @return Location of password file.
     */
    String getUserPasswordFile();

    /**
     * A configuration to switch TLS on client to controller interactions.
      * @return A flag representing TLS status.
     */
    boolean isTlsEnabled();

    /**
     * The truststore to be used while talking to segmentstore over TLS.
     *
     * @return A path pointing to a trust store.
     */
    String getTlsTrustStore();

    /**
     * X.509 certificate file to be used for TLS.
     *
     * @return A file which contains the TLS certificate.
     */
    String getTlsCertFile();


    /**
     * File containing the private key for the X.509 certificate used for TLS.
     * @return A file which contains the private key for the TLS certificate.
     */
    String getTlsKeyFile();

    /**
     * Returns the key that is shared between segmentstore and controller and is used for
     * signing the delegation token.
     * @return The string to be used for signing the token.
     */
    String getTokenSigningKey();

    /**
     * Returns the access token time-to-live value.
     *
     * @return time-to-live value of the access token in seconds.
     */
    int getAccessTokenTtlInSeconds();

    /**
     * Returns whether the controller should send back to the client a full stack trace describing an error upon a
     * failed request or not. This may ease debugging tasks just inspecting client logs at the cost of exposing server-
     * side information to the client side, which may be sensitive.
     *
     * @return Whether or not to reply the client with server-side stack traces upon errors.
     */
    boolean isReplyWithStackTraceOnError();

    /**
     * Returns whether the Controller should keep track of client request identifiers for using them internally and
     * in its own requests against the Segment Store. This allows us to have an end-to-end visibility of the activity
     * generated by a client's request.
     *
     * @return Whether or not to activate client request tracing.
     */
    boolean isRequestTracingEnabled();
}
