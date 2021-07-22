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
package io.pravega.controller.server.rpc.grpc.impl;

import io.pravega.auth.AuthPluginConfig;
import org.junit.Rule;
import org.junit.Test;
import io.pravega.controller.server.rpc.grpc.impl.GRPCServerConfigImpl.GRPCServerConfigImplBuilder;
import org.junit.rules.Timeout;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;


public class GRPCServerConfigImplTest {

    // region Tests that verify the toString() method.

    // Note: It might seem odd that we are unit testing the toString() method of the code under test. The reason we are
    // doing that is that the method is hand-rolled and there is a bit of logic there that isn't entirely unlikely to fail.
    @Rule
    public Timeout globalTimeout = new Timeout(30, TimeUnit.SECONDS);

    @Test
    public void testToStringReturnsSuccessfullyWithAllConfigSpecified() {
        GRPCServerConfigImpl config = new GRPCServerConfigImplBuilder().port(9090)
                .publishedRPCHost("localhost")
                .publishedRPCPort(9090)
                .authorizationEnabled(true)
                .userPasswordFile("/passwd")
                .tlsEnabled(true)
                .tlsCertFile("/cert.pem")
                .tlsKeyFile("./key.pem")
                .tokenSigningKey("secret")
                .accessTokenTTLInSeconds(200)
                .tlsTrustStore("/cert.pem")
                .replyWithStackTraceOnError(true)
                .isRGWritesWithReadPermEnabled(true)
                .requestTracingEnabled(true)
                .build();
        assertNotNull(config.toString());
    }

    @Test
    public void testToStringReturnsSuccessfullyWithSomeConfigNullOrEmpty() {
        GRPCServerConfigImpl config = new GRPCServerConfigImplBuilder().port(9090)
                .publishedRPCHost(null)
                .publishedRPCPort(9090)
                .authorizationEnabled(true)
                .userPasswordFile(null)
                .tlsEnabled(true)
                .tlsCertFile("")
                .tlsKeyFile("")
                .tokenSigningKey("secret")
                .accessTokenTTLInSeconds(300)
                .isRGWritesWithReadPermEnabled(false)
                .tlsTrustStore("/cert.pem")
                .replyWithStackTraceOnError(false)
                .requestTracingEnabled(true)
                .build();
        assertNotNull(config.toString());
    }

    @Test
    public void testToStringReturnsSuccessfullyWithAccessTokenTtlNull() {
        GRPCServerConfigImpl config = new GRPCServerConfigImplBuilder().port(9090)
                .publishedRPCHost("localhost")
                .publishedRPCPort(9090)
                .authorizationEnabled(true)
                .userPasswordFile("/passwd")
                .tlsEnabled(true)
                .tlsCertFile("/cert.pem")
                .tlsKeyFile("./key.pem")
                .tokenSigningKey("secret")
                .accessTokenTTLInSeconds(null)
                .isRGWritesWithReadPermEnabled(true)
                .tlsTrustStore("/cert.pem")
                .replyWithStackTraceOnError(true)
                .requestTracingEnabled(true)
                .build();
        assertNotNull(config.toString());
    }

    @Test
    public void testToAuthPluginPropertiesWhenConfigHasUSerPasswordFile() {
        Properties props = new GRPCServerConfigImplBuilder().port(9090)
                .publishedRPCHost("localhost")
                .publishedRPCPort(9090)
                .authorizationEnabled(true)
                .userPasswordFile("/passwd")
                .tlsEnabled(true)
                .tlsCertFile("/cert.pem")
                .tlsKeyFile("./key.pem")
                .tokenSigningKey("secret")
                .accessTokenTTLInSeconds(null)
                .isRGWritesWithReadPermEnabled(true)
                .tlsTrustStore("/cert.pem")
                .replyWithStackTraceOnError(true)
                .requestTracingEnabled(true)
                .build().toAuthHandlerProperties();
        assertNotNull(props);
        assertNotNull(props.get(AuthPluginConfig.BASIC_AUTHPLUGIN_DATABASE));
    }

    @Test
    public void testToAuthPluginPropertiesReturnsEmptyWhenUserPasswordFileIsAbsent() {
        Properties props = new GRPCServerConfigImplBuilder().port(9090)
                .publishedRPCHost("localhost")
                .publishedRPCPort(9090)
                .authorizationEnabled(true)
                //.userPasswordFile("/passwd")
                .tlsEnabled(true)
                .tlsCertFile("/cert.pem")
                .tlsKeyFile("./key.pem")
                .tokenSigningKey("secret")
                .accessTokenTTLInSeconds(null)
                .isRGWritesWithReadPermEnabled(true)
                .tlsTrustStore("/cert.pem")
                .replyWithStackTraceOnError(true)
                .requestTracingEnabled(true)
                .build().toAuthHandlerProperties();
        assertNotNull(props);
        assertNull(props.get(AuthPluginConfig.BASIC_AUTHPLUGIN_DATABASE));
    }

    // endregion
}
