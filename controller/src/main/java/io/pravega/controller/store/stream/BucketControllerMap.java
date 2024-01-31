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

package io.pravega.controller.store.stream;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.SneakyThrows;

/**
 * BucketControllerMap keeps the buckets to controller mapping where process id is key and bucket is set as value.
 * ProcessId keeps host id of controller and BucketSet keeps id of buckets assigned to controller instance.
 */
public class BucketControllerMap {
    public static final BucketControllerMapSerializer SERIALIZER = new BucketControllerMapSerializer();
    public static final BucketControllerMap EMPTY = new BucketControllerMap(Collections.emptyMap());

    private final Map<ProcessId, BucketSet> bucketControllerMap;

    @Builder
    public BucketControllerMap(Map<ProcessId, BucketSet> map) {
        Preconditions.checkNotNull(map);
        this.bucketControllerMap = ImmutableMap.copyOf(map);
    }

    public static BucketControllerMap createBucketControllerMap(Map<String, Set<Integer>> map) {
        Preconditions.checkNotNull(map);
        return new BucketControllerMap(map.entrySet().stream().collect(Collectors
                .toMap(y -> new ProcessId(y.getKey()), x -> new BucketSet(ImmutableSet.copyOf(x.getValue())))));
    }

    @SneakyThrows(IOException.class)
    public static BucketControllerMap fromBytes(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }

    public static class BucketControllerMapBuilder implements ObjectBuilder<BucketControllerMap> {

    }

    public Map<String, Set<Integer>> getBucketControllerMap() {
        return bucketControllerMap.entrySet().stream().collect(Collectors.toMap(y -> y.getKey().getProcessId(), x -> x.getValue().getBucketSet()));
    }

    private static class BucketControllerMapSerializer
            extends VersionedSerializer.WithBuilder<BucketControllerMap, BucketControllerMapBuilder> {

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, BucketControllerMapBuilder bucketControllerBuilder)
                throws IOException {
            bucketControllerBuilder
                    .map(revisionDataInput.readMap(ProcessId.SERIALIZER::deserialize, BucketSet.SERIALIZER::deserialize));
        }

        private void write00(BucketControllerMap bucketControllerMap, RevisionDataOutput revisionDataOutput)
                throws IOException {
            revisionDataOutput.writeMap(bucketControllerMap.bucketControllerMap, ProcessId.SERIALIZER::serialize,
                    BucketSet.SERIALIZER::serialize);
        }

        @Override
        protected BucketControllerMapBuilder newBuilder() {
            return BucketControllerMap.builder();
        }
    }
}
