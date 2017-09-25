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

import java.util.concurrent.CompletableFuture;

public interface RequestProcessor {
    CompletableFuture<Void> processAbortTxnRequest(AbortEvent abortEvent);
    CompletableFuture<Void> processCommitTxnRequest(CommitEvent commitEvent);
    CompletableFuture<Void> processAutoScaleRequest(AutoScaleEvent autoScaleEvent);
    CompletableFuture<Void> processScaleOpRequest(ScaleOpEvent scaleOpEvent);
    CompletableFuture<Void> processUpdateStream(UpdateStreamEvent updateStreamEvent);
    CompletableFuture<Void> processSealStream(SealStreamEvent sealStreamEvent);
    CompletableFuture<Void> processDeleteStream(DeleteStreamEvent deleteStreamEvent);
}
