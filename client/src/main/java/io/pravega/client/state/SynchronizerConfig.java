/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.state;

import io.pravega.client.stream.EventWriterConfig;
import java.io.Serializable;
import lombok.Builder;
import lombok.Data;

/**
 * The configuration for a Consistent replicated state synchronizer.
 */
@Data
@Builder
public class SynchronizerConfig implements Serializable {

    private static final long serialVersionUID = 1L;
    EventWriterConfig eventWriterConfig;
    
    public static class SynchronizerConfigBuilder {
        private EventWriterConfig eventWriterConfig = EventWriterConfig.builder().enableConnectionPooling(true).build();
    }
}
