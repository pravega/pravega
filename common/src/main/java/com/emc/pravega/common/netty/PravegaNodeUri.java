package com.emc.pravega.common.netty;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;

@Data
@AllArgsConstructor
public class PravegaNodeUri {
    @NonNull
    private final String endpoint;
    private final int port;
}
