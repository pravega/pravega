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
package io.pravega.controller.store.stream.records;

import com.google.common.collect.ImmutableSet;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;

import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Data class containing the list of subscribers registered to a Stream.
 */
@Slf4j
@Data
public class Subscribers {
    public static final Subscribers EMPTY_SET = new Subscribers(ImmutableSet.of());
    public static final SubscriberSetSerializer SERIALIZER = new SubscriberSetSerializer();

    @Getter
    private final ImmutableSet<String> subscribers;

    @Builder
    public Subscribers(@NonNull ImmutableSet<String> subscribers) {
        this.subscribers = subscribers;
    }

    /**
     * This method adds a new subscriber to the subscriberSet.
     * @param subscriberSet Subscriber Set.
     * @param subscriber subscriber to be added.
     * @return updated Subscriber Set.
     */
    public static Subscribers add(@NonNull Subscribers subscriberSet, @NonNull String subscriber) {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        builder.addAll(subscriberSet.subscribers);
        builder.add(subscriber);
        return new Subscribers(builder.build());
    }

    /**
     * This method removes a subscriber from the subscriberSet.
     * @param subscriberSet Subscriber Set.
     * @param subscriber subscriber to be removed.
     * @return updated Subscriber Set.
     */
    public static Subscribers remove(@NonNull Subscribers subscriberSet, @NonNull String subscriber) {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        subscriberSet.getSubscribers().forEach(s -> {
            if (!s.equals(subscriber)) {
                builder.add(s);
            }
        });
        return new Subscribers(builder.build());
    }

    private static class SubscribersBuilder implements ObjectBuilder<Subscribers> {
    }

    @SneakyThrows(IOException.class)
    public static Subscribers fromBytes(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }

    @Override
    public String toString() {
        return String.format("%s = %s", "subscribers", subscribers);
    }

    private static class SubscriberSetSerializer
            extends VersionedSerializer.WithBuilder<Subscribers, Subscribers.SubscribersBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, Subscribers.SubscribersBuilder recordBuilder)
                throws IOException {
            ImmutableSet.Builder<String> builder = ImmutableSet.builder();
            revisionDataInput.readCollection(DataInput::readUTF, builder);
            recordBuilder.subscribers(builder.build());
        }

        private void write00(Subscribers subscribersRecord, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeCollection(subscribersRecord.getSubscribers(), DataOutput::writeUTF);
        }

        @Override
        protected Subscribers.SubscribersBuilder newBuilder() {
            return Subscribers.builder();
        }
    }
}