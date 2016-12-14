package com.emc.pravega.stream;

import java.io.Serializable;

import lombok.Data;

@Data
public class RetentionPolicy implements Serializable {
    private final long retentionTimeMillis;
}
