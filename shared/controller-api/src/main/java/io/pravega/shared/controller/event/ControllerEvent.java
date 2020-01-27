/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.controller.event;

import java.util.concurrent.CompletableFuture;

public interface ControllerEvent {
    /**
     * Method to get routing key for the event.
     * @return return the routing key that should be used.
     */
    String getKey();

    CompletableFuture<Void> process(RequestProcessor processor);
}
