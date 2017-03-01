package com.emc.pravega.stream.impl;

import com.emc.pravega.stream.Checkpoint;
import com.emc.pravega.stream.Segment;
import java.util.Map;
import lombok.Data;

@Data
public class CheckpointImpl implements Checkpoint {

    private static final long serialVersionUID = 1L;
    private final String name;
    private final Map<Segment, Long> positions;

}
