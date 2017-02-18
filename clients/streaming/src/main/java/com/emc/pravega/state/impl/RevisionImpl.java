/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.state.impl;

import com.emc.pravega.state.Revision;
import com.emc.pravega.stream.Segment;
import com.google.common.base.Preconditions;

import java.io.Serializable;

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@EqualsAndHashCode
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
@ToString
public class RevisionImpl implements Revision, Serializable {

    @Getter(value = AccessLevel.PACKAGE)
    private final Segment segment;
    @Getter(value = AccessLevel.PACKAGE)
    private final long offsetInSegment;
    @Getter(value = AccessLevel.PACKAGE)
    private final int eventAtOffset;

    @Override
    public int compareTo(Revision o) {
        Preconditions.checkArgument(segment.equals(o.asImpl().getSegment()));
        int result = Long.compare(offsetInSegment, o.asImpl().offsetInSegment);
        return result != 0 ? result : Integer.compare(eventAtOffset, o.asImpl().eventAtOffset); 
    }

    @Override
    public RevisionImpl asImpl() {
        return this;
    }

}
