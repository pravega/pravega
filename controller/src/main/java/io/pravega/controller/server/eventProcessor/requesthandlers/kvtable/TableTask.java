/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
<<<<<<< HEAD
<<<<<<< HEAD
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
=======
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
>>>>>>> Issue 4796: (KeyValue Tables) CreateAPI for Key Value Tables (#4797)
=======
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
>>>>>>> Minor things.
 */
package io.pravega.controller.server.eventProcessor.requesthandlers.kvtable;

import io.pravega.shared.controller.event.ControllerEvent;

import java.util.concurrent.CompletableFuture;

public interface TableTask<T extends ControllerEvent> {

    /**
     * Method to process the supplied event.
     * @param event event to process
     * @return future of processing
     */
    CompletableFuture<Void> execute(T event);

}
