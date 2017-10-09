/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.notifications.notifier;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import io.pravega.client.stream.notifications.Listener;
import io.pravega.client.stream.notifications.NotificationSystem;
import io.pravega.client.stream.notifications.Observable;
import io.pravega.client.stream.notifications.events.ScaleEvent;

public class ScaleEventNotifier implements Observable<ScaleEvent> {

    private static final int UPDATE_INTERVAL_SECONDS = Integer.parseInt(
            System.getProperty("pravega.client.scaleEvent.poll.interval.seconds", String.valueOf(120)));
    private final NotificationSystem system;
    private final Supplier<ScaleEvent> scaleEventSupplier;
    private final AtomicBoolean pollingStarted = new AtomicBoolean();

    public ScaleEventNotifier(final NotificationSystem system, Supplier<ScaleEvent> scaleEventSupplier) {
        this.system = system;
        this.scaleEventSupplier = scaleEventSupplier;
    }

    @Override
    public void addListener(final Listener<ScaleEvent> listener, final ScheduledExecutorService executor) {
        system.addListeners(getType(), listener, executor);
        //periodically fetch the scale

        if (!pollingStarted.getAndSet(true)) { //schedule the  only once
            executor.scheduleAtFixedRate(this::checkAndTriggerScaleNotification, 0,
                    UPDATE_INTERVAL_SECONDS, TimeUnit.SECONDS);
        }
    }

    private void checkAndTriggerScaleNotification() {
        ScaleEvent event = scaleEventSupplier.get();
        if (event.getNumOfReaders() != event.getNumOfSegments()) {
            system.notify(event);
        }
    }

    @Override
    public void removeListener(final Listener<ScaleEvent> listener) {
        system.removeListeners(listener);
    }

    @Override
    public void removeListener() {
        this.system.removeListeners(getType());
    }

    @Override
    public Class<ScaleEvent> getType() {
        return ScaleEvent.class;
    }
}
