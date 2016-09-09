package com.emc.pravega.stream;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SegmentUri {
    private final String endpoint;
    private final int port;
}
