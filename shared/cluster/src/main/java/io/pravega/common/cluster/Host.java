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
package io.pravega.common.cluster;

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.SneakyThrows;

import java.io.IOException;

@Data
public class Host {
    static final HostSerializer SERIALIZER = new HostSerializer();

    @NonNull
    private final String ipAddr;
    private final int port;
    private final String endpointId;
    private final String hostId;
    
    @Builder
    public Host(String ipAddr, int port, String endpointId) {
        this.ipAddr = ipAddr;
        this.port = port;
        this.endpointId = endpointId == null ? "" : endpointId;
        hostId = String.format("%s-%s", this.ipAddr, this.endpointId); 
    }

    @Override
    public String toString() {
        String endpoint = this.endpointId == null ? "" : String.format(":%s", endpointId);
        return String.format("%s:%d%s", this.getIpAddr(), this.getPort(), endpoint);
    }

    public static class HostBuilder implements ObjectBuilder<Host> {

    }

    @SneakyThrows(IOException.class)
    public static Host fromBytes(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }

    static class HostSerializer
            extends VersionedSerializer.WithBuilder<Host, HostBuilder> {

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, HostBuilder hostBuilder) throws IOException {
            hostBuilder.ipAddr(revisionDataInput.readUTF())
                       .port(revisionDataInput.readInt())
                       .endpointId(revisionDataInput.readUTF());
        }

        private void write00(Host host, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeUTF(host.ipAddr);
            revisionDataOutput.writeInt(host.port);
            revisionDataOutput.writeUTF(host.endpointId);
        }

        @Override
        protected HostBuilder newBuilder() {
            return Host.builder();
        }
    }
}
