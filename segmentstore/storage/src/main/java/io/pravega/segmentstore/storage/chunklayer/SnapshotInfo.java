/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.chunklayer;

import lombok.Builder;
import lombok.Data;

/**
 * Basic info about snapshot.
 */
@Data
@Builder
public class SnapshotInfo {
    /**
     * Epoch.
     */
    final private long epoch;

    /**
     * Id of the snapshot.
     */
    final private long snapshotId;
}
