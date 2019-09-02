/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server;

import io.pravega.common.util.AsyncIterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Defines an {@link AsyncIterator} over a Segment's Attributes, processing them in batches.
 */
public interface AttributeIterator extends AsyncIterator<List<Map.Entry<UUID, Long>>> {
}