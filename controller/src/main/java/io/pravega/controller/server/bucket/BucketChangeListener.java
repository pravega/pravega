/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.bucket;

import lombok.Data;

@FunctionalInterface
public interface BucketChangeListener {
    void notify(StreamNotification notification);

    @Data
    class StreamNotification {
        private final String scope;
        private final String stream;
        private final NotificationType type;

        public enum NotificationType {
            StreamAdded,
            StreamRemoved,
            StreamUpdated,
            ConnectivityError
        }
    }
}
