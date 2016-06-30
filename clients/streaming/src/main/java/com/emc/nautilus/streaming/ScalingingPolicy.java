package com.emc.nautilus.streaming;

import lombok.Data;

@Data
public class ScalingingPolicy { // TODO
    enum Type {
        FIXED,
        BY_RATE
    }

    private final Type type;
    private final long value;
}
