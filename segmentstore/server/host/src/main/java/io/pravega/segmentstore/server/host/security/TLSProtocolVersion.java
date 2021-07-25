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
package io.pravega.segmentstore.server.host.security;

import java.util.Arrays;

public class TLSProtocolVersion {

    enum TlsProtocolVersion {

        TLSv1_2("TLSv1.2"),
        TLSv1_3("TLSv1.3"),
        TLS1_2ANDTLS1_3("TLSv1.2,TLSv1.3"),
        TLS1_3ANDTLS1_2("TLSv1.3,TLSv1.2");

        private String tlsprotocol;

        TlsProtocolVersion(String tlsprotocol) {
            this.tlsprotocol = tlsprotocol;
        }

        static boolean parse(String s) {
            return Arrays.stream(TlsProtocolVersion.values()).anyMatch(e -> e.tlsprotocol.equals(s));
        }
    }

    public static String[] getTlsProtocolVersionList(String s) {
        return s.split(",");
    }
}