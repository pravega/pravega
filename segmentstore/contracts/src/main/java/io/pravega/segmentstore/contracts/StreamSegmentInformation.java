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
package io.pravega.segmentstore.contracts;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.util.ImmutableDate;
import java.util.Collections;
import java.util.Map;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * General Stream Segment Information.
 */
@EqualsAndHashCode
public class StreamSegmentInformation implements SegmentProperties {
    //region Members

    @Getter
    private final String name;
    @Getter
    private final long startOffset;
    @Getter
    private final long length;
    @Getter
    private final boolean sealed;
    @Getter
    private final boolean deleted;
    @Getter
    private final boolean sealedInStorage;
    @Getter
    private final boolean deletedInStorage;
    @Getter
    private final long storageLength;

    @Getter
    private final ImmutableDate lastModified;
    @Getter
    private final Map<AttributeId, Long> attributes;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentInformation class.
     *
     * @param name             The name of the StreamSegment.
     * @param startOffset      The first available offset in this StreamSegment.
     * @param length           The length of the StreamSegment.
     * @param sealed           Whether the StreamSegment is sealed (for modifications).
     * @param deleted          Whether the StreamSegment is deleted (does not exist).
     * @param storageLength    Storage length.
     * @param sealedInStorage  Whether the StreamSegment is sealed (for modifications) in storage.
     * @param deletedInStorage Whether the StreamSegment is deleted (does not exist) in storage.
     * @param attributes       The attributes of this StreamSegment.
     * @param lastModified     The last time the StreamSegment was modified.
     */
    @Builder
    private StreamSegmentInformation(String name, long startOffset, long length, long storageLength, boolean sealed, boolean deleted,
                                     boolean sealedInStorage, boolean deletedInStorage, Map<AttributeId, Long> attributes, ImmutableDate lastModified) {
        Preconditions.checkArgument(startOffset >= 0, "startOffset must be a non-negative number.");
        Preconditions.checkArgument(length >= startOffset, "length must be a non-negative number and greater than startOffset.");
        Preconditions.checkArgument(length >= storageLength, "storageLength %s must be less than or equal to length %s for segment %s.", storageLength, length, name);
        if (deletedInStorage) {
            Preconditions.checkArgument(deleted, "deleted must be set if deletedInStorage is set.");
        }
        if (sealedInStorage) {
            Preconditions.checkArgument(sealed, "sealed must be set if sealedInStorage is set.");
        }
        this.name = Exceptions.checkNotNullOrEmpty(name, "name");
        this.startOffset = startOffset;
        this.length = length;
        this.storageLength = storageLength;
        this.sealed = sealed;
        this.sealedInStorage = sealedInStorage;
        this.deleted = deleted;
        this.deletedInStorage = deletedInStorage;
        this.lastModified = lastModified == null ? new ImmutableDate() : lastModified;
        this.attributes = createAttributes(attributes);
    }

    /**
     * Creates a new {@link StreamSegmentInformationBuilder} with information already populated from the given SegmentProperties.
     *
     * @param base The SegmentProperties to use as a base.
     * @return The Builder.
     */
    public static StreamSegmentInformationBuilder from(SegmentProperties base) {
        return StreamSegmentInformation.builder()
                                       .name(base.getName())
                                       .startOffset(base.getStartOffset())
                                       .length(base.getLength())
                                       .sealed(base.isSealed())
                                       .deleted(base.isDeleted())
                                       .lastModified(base.getLastModified())
                                       .attributes(base.getAttributes());
    }

    //endregion

    @Override
    public String toString() {
        return String.format("Name = %s, StartOffset = %d, Length = %d, Storage Length = %d, Sealed = %s, Deleted = %s, Sealed in Storage = %s, Deleted in Storage = %s", getName(),
                getStartOffset(), getLength(), getStorageLength(), isSealed(), isDeleted(), isSealedInStorage(), isDeletedInStorage());
    }

    private static Map<AttributeId, Long> createAttributes(Map<AttributeId, Long> input) {
        return input == null || input.size() == 0 ? Collections.emptyMap() : Collections.unmodifiableMap(input);
    }
}
