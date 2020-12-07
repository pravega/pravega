/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.tables.KeyValueTableConfiguration;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class CreateRGResponse {
    public enum CreateStatus {
        NEW,
        EXISTS_CREATING,
        EXISTS_ACTIVE,
        FAILED
    }

    private final CreateStatus status;
    private final ReaderGroupConfig configuration;
    private final long timestamp;
    private final int startingSegmentNumber;
}
