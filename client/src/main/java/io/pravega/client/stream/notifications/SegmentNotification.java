/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.notifications;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Class to represent a segment notification. This contains the current number of segments and the current number of
 * readers.
 */
@Data
@EqualsAndHashCode(callSuper = false)
@Builder
public class SegmentNotification extends Notification {
    private int numOfSegments;
    private int numOfReaders;
}
