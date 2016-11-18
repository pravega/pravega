package com.emc.pravega.state.impl;

import com.emc.pravega.state.Revision;

import lombok.Data;

@Data
public class RevisionImpl implements Revision {

    private final long offsetInSegment;
    
    @Override
    public int compareTo(Revision o) {
        return Long.compare(offsetInSegment, o.asImpl().offsetInSegment);
    }

    @Override
    public RevisionImpl asImpl() {
        return this;
    }

}
