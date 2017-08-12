/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.eventProcessor;

import io.pravega.shared.controller.event.StreamEvent;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.AbstractMap;
import java.util.List;

@Data
@EqualsAndHashCode(callSuper = false)
public class ScaleOpEvent extends StreamEvent {
    private final List<Integer> segmentsToSeal;
    private final List<AbstractMap.SimpleEntry<Double, Double>> newRanges;
    private final boolean runOnlyIfStarted;
    private final long scaleTime;

    public ScaleOpEvent(String scope, String stream, List<Integer> segmentsToSeal,
                        List<AbstractMap.SimpleEntry<Double, Double>> newRanges, boolean runOnlyIfStarted, long scaleTime) {
        super(scope, stream);
        this.segmentsToSeal = segmentsToSeal;
        this.newRanges = newRanges;
        this.runOnlyIfStarted = runOnlyIfStarted;
        this.scaleTime = scaleTime;
    }
}
