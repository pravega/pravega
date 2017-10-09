/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.notifications.events;

import io.pravega.client.stream.notifications.Event;
import lombok.Builder;
import lombok.Data;

/**
 * Class to represent a scale event.
 */
@Data
@Builder
public class ScaleEvent implements Event {
    private int numOfSegments;
    private int numOfReaders;
}
