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

import com.google.common.util.concurrent.Service;

/**
 * Defines a component that pulls data from an OperationLog and writes it to a Storage. This is a background service that
 * does not expose any APIs, except for those controlling its lifecycle.
 */
public interface Writer extends Service, AutoCloseable {
    @Override
    void close();
}
