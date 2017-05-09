/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.state.impl;

import com.google.common.base.Preconditions;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.state.Revision;
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

    private static final long serialVersionUID = 1L;
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
