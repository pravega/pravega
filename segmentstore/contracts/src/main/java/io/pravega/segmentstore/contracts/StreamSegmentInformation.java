/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.contracts;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.util.ImmutableDate;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import lombok.Builder;
import lombok.Getter;

/**
 * General Stream Segment Information.
 */
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
    private final ImmutableDate lastModified;
    @Getter
    private final Map<UUID, Long> attributes;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentInformation class.
     *
     * @param name         The name of the StreamSegment.
     * @param startOffset  The first available offset in this StreamSegment.
     * @param length       The length of the StreamSegment.
     * @param sealed       Whether the StreamSegment is sealed (for modifications).
     * @param deleted      Whether the StreamSegment is deleted (does not exist).
     * @param attributes   The attributes of this StreamSegment.
     * @param lastModified The last time the StreamSegment was modified.
     */
    @Builder
    private StreamSegmentInformation(String name, long startOffset, long length, boolean sealed, boolean deleted,
                                    Map<UUID, Long> attributes, ImmutableDate lastModified) {
        Preconditions.checkArgument(startOffset >= 0, "startOffset must be a non-negative number.");
        Preconditions.checkArgument(length >= startOffset, "length must be a non-negative number and greater than startOffset.");
        this.name = Exceptions.checkNotNullOrEmpty(name, "name");
        this.startOffset = startOffset;
        this.length = length;
        this.sealed = sealed;
        this.deleted = deleted;
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
        return String.format("Name = %s, StartOffset = %d, Length = %d, Sealed = %s, Deleted = %s", getName(),
                getStartOffset(), getLength(), isSealed(), isDeleted());
    }

    private static Map<UUID, Long> createAttributes(Map<UUID, Long> input) {
        return input == null || input.size() == 0 ? Collections.emptyMap() : Collections.unmodifiableMap(input);
    }
}
