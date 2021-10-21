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

import com.google.common.collect.ImmutableMap;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.Builder;
import lombok.SneakyThrows;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class HostContainerMap {
    public static final HostContainerMapSerializer SERIALIZER = new HostContainerMapSerializer();
    public static final HostContainerMap EMPTY = new HostContainerMap(Collections.emptyMap());
    
    private final Map<Host, ContainerSet> map;

    @Builder
    public HostContainerMap(Map<Host, ContainerSet> map) {
        this.map = ImmutableMap.copyOf(map);
    }

    public static HostContainerMap createHostContainerMap(Map<Host, Set<Integer>> map) {
        return new HostContainerMap(map.entrySet().stream().collect(Collectors
                .toMap(Map.Entry::getKey, x -> new ContainerSet(x.getValue()))));
    }
    
    @SneakyThrows(IOException.class)
    public static HostContainerMap fromBytes(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }

    public static class HostContainerMapBuilder implements ObjectBuilder<HostContainerMap> {

    }

    public Map<Host, Set<Integer>> getHostContainerMap() {
        return map.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, x -> x.getValue().getContainerSet()));
    }
    
    private static class HostContainerMapSerializer
            extends VersionedSerializer.WithBuilder<HostContainerMap, HostContainerMapBuilder> {

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, HostContainerMapBuilder hostContainerMapBuilder) throws IOException {
            hostContainerMapBuilder
                    .map(revisionDataInput.readMap(Host.SERIALIZER::deserialize, ContainerSet.SERIALIZER::deserialize));
        }

        private void write00(HostContainerMap hostContainerMap, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeMap(hostContainerMap.map, Host.SERIALIZER::serialize,
                    ContainerSet.SERIALIZER::serialize);
        }

        @Override
        protected HostContainerMapBuilder newBuilder() {
            return HostContainerMap.builder();
        }
    }
}
