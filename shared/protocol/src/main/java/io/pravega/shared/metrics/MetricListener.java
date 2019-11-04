/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.shared.metrics;

public interface MetricListener extends AutoCloseable {

    /**
     * An operation with the given value succeeded.
     *
     * @param operation The operation
     * @param value the value
     */
    void reportSuccessValue(String operation, long value);

    /**
     * An operation with the given value failed.
     *
     * @param operation The operation
     * @param value the value
     */
    void reportFailValue(String operation, long value);

    @Override
    void close();

}
