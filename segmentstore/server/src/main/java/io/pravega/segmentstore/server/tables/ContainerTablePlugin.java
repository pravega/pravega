/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables;

import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.SegmentContainerPlugin;

/**
 * Defines a Segment Container Plugin that implements Tables on top of Segment Containers.
 */
public interface ContainerTablePlugin extends TableStore, SegmentContainerPlugin {

}