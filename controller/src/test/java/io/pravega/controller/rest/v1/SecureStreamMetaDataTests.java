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
package io.pravega.controller.rest.v1;

import io.pravega.test.common.SecurityConfigDefaults;
import io.pravega.shared.rest.security.AuthHandlerManager;
import io.pravega.shared.security.crypto.StrongPasswordProcessor;
import io.pravega.controller.server.rpc.grpc.impl.GRPCServerConfigImpl;
import io.pravega.test.common.TestUtils;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Before;

import static io.pravega.auth.AuthFileUtils.addAuthFileEntry;

public class SecureStreamMetaDataTests extends  StreamMetaDataTests {

    @Override
    @Before
    public void setup() throws Exception {
        File file = File.createTempFile("SecureStreamMetaDataTests", ".txt");
        StrongPasswordProcessor passwordEncryptor = StrongPasswordProcessor.builder().build();

        try (FileWriter writer = new FileWriter(file.getAbsolutePath())) {
            String passwd = passwordEncryptor.encryptPassword("1111_aaaa");

            // Admin has READ_WRITE permission to everything
            addAuthFileEntry(writer, "admin", passwd, Collections.singletonList("prn::*,READ_UPDATE"));

            // User "user1" can:
            //    - list, create and delete scopes
            //    - Create and delete streams within scopes "scope1" and "scope2". Also if "user1" lists scopes,
            //      she'll see those scopes, but not "scope3".
            addAuthFileEntry(writer, "user1", passwd, Arrays.asList(
                    "prn::/,READ_UPDATE",
                    "prn::/scope:scope1,READ_UPDATE",
                    "prn::/scope:scope1/*,READ_UPDATE",
                    "prn::/scope:scope2,READ_UPDATE",
                    "prn::/scope:scope2/*,READ_UPDATE\n"
            ));

            addAuthFileEntry(writer, "user2", passwd, Arrays.asList(
                    "prn::/,READ",
                    "prn::/scope:scope3,READ_UPDATE"));
        }

        this.authManager = new AuthHandlerManager(GRPCServerConfigImpl.builder()
                                                                      .authorizationEnabled(true)
                                                                      .tlsCertFile(SecurityConfigDefaults.TLS_SERVER_CERT_PATH)
                                                                      .tlsKeyFile(SecurityConfigDefaults.TLS_SERVER_PRIVATE_KEY_PATH)
                                                                      .userPasswordFile(file.getAbsolutePath())
                                                                      .port(1000)
                                                                      .build());
        super.setup();
    }

    @Override
    protected Invocation.Builder addAuthHeaders(Invocation.Builder request) {
        MultivaluedMap<String, Object> map = new MultivaluedHashMap<>();
        map.addAll(HttpHeaders.AUTHORIZATION, TestUtils.basicAuthToken("admin", "1111_aaaa"));
        return request.headers(map);
    }
}
