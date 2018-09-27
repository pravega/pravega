/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.controller.event;

import lombok.Data;

import java.util.AbstractMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Data
public class ScaleOpEvent implements ControllerEvent {
    private static final long serialVersionUID = 1L;
    private final String scope;
    private final String stream;
    private final List<Long> segmentsToSeal;
    private final List<AbstractMap.SimpleEntry<Long, Long>> newRanges;
    private final boolean runOnlyIfStarted;
    private final long scaleTime; // scaleTime is an equivalent for requestId

    public ScaleOpEvent(String scope, String stream, List<Long> segmentsToSeal, List<AbstractMap.SimpleEntry<Double, Double>> newRange,
                        boolean runOnlyIfStarted, long scaleTime) {
        this.scope = scope;
        this.stream = stream;
        this.segmentsToSeal = segmentsToSeal;
        this.newRanges = newRange.stream()
                .map(x -> new AbstractMap.SimpleEntry<>(Double.doubleToRawLongBits(x.getKey()), Double.doubleToRawLongBits(x.getValue())))
                .collect(Collectors.toList());
        this.runOnlyIfStarted = runOnlyIfStarted;
        this.scaleTime = scaleTime;
    }

    public List<AbstractMap.SimpleEntry<Double, Double>> getNewRanges() {
        return newRanges.stream()
                .map(x -> new AbstractMap.SimpleEntry<>(Double.longBitsToDouble(x.getKey()), Double.longBitsToDouble(x.getValue())))
                .collect(Collectors.toList());
    }

    @Override
    public String getKey() {
        return String.format("%s/%s", scope, stream);
    }

    @Override
    public CompletableFuture<Void> process(RequestProcessor processor) {
        return processor.processScaleOpRequest(this);
    }
}
