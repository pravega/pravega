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

import com.google.common.collect.ImmutableSet;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import lombok.Builder;
import lombok.Getter;

/**
 * BucketSet hold set of bucket Ids.
 */
@Builder
public class BucketSet {
    static final BucketSet.BucketSetSerializer SERIALIZER = new BucketSet.BucketSetSerializer();

    @Getter
    private final ImmutableSet<Integer> bucketSet;

    public static class BucketSetBuilder implements ObjectBuilder<BucketSet> {

    }

    static class BucketSetSerializer
            extends VersionedSerializer.WithBuilder<BucketSet, BucketSet.BucketSetBuilder> {

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, BucketSet.BucketSetBuilder bucketSetBuilder) throws IOException {
            ImmutableSet.Builder<Integer> builder = ImmutableSet.builder();
            revisionDataInput.readCollection(DataInput::readInt, builder);
            bucketSetBuilder.bucketSet(builder.build());
        }

        private void write00(BucketSet bucketSet, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeCollection(bucketSet.getBucketSet(), DataOutput::writeInt);
        }

        @Override
        protected BucketSet.BucketSetBuilder newBuilder() {
            return BucketSet.builder();
        }
    }
}
