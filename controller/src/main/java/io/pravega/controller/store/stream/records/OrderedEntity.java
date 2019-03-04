/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream.records;

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.IOException;

@Data
@Builder
@AllArgsConstructor
public class OrderedEntity<T> implements Comparable<OrderedEntity> {
    public static final OrderedEntitySerializer SERIALIZER = new OrderedEntitySerializer();

    private final Integer queueNumber;
    private final Integer positionInQueue;

    @Override
    public int compareTo(OrderedEntity o) {
        if (this.queueNumber < o.queueNumber) {
            return -1;
        } else if (this.queueNumber.equals(o.queueNumber)) {
            return Integer.compare(this.positionInQueue, o.positionInQueue);
        } else {
            return 1;
        }
    }

    public static class OrderedEntityBuilder implements ObjectBuilder<OrderedEntity> {
    }

    public static class OrderedEntitySerializer
            extends VersionedSerializer.WithBuilder<OrderedEntity, OrderedEntity.OrderedEntityBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, OrderedEntityBuilder orderedEntityBuilder) throws IOException {
            orderedEntityBuilder.queueNumber(revisionDataInput.readInt())
                                .positionInQueue(revisionDataInput.readInt());
                                
        }

        private void write00(OrderedEntity orderedEntity, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeInt(orderedEntity.queueNumber);
            revisionDataOutput.writeInt(orderedEntity.positionInQueue);
        }

        @Override
        protected OrderedEntityBuilder newBuilder() {
            return OrderedEntity.builder();
        }
    }
}