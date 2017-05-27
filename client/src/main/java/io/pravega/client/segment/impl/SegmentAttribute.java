package io.pravega.client.segment.impl;

import io.pravega.common.hash.HashHelper;
import io.pravega.shared.protocol.netty.WireCommands;
import java.util.UUID;
import lombok.Getter;

public enum SegmentAttribute {

    RevisionStreamClientMark;
    
    public static final long NULL_VALUE = WireCommands.NULL_ATTRIBUTE_VALUE;
    
    @Getter
    private final UUID value;
    
    SegmentAttribute() {
        HashHelper hash = HashHelper.seededWith("SegmentAttribute");
        value = hash.toUUID(this.name());
    }
}
