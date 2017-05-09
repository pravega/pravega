/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.service.server.logs;

import io.pravega.service.server.LogItemFactory;
import io.pravega.test.common.ErrorInjector;

import java.io.InputStream;

/**
 * LogItemFactory for TestLogItem.
 */
public class TestLogItemFactory implements LogItemFactory<TestLogItem> {
    private ErrorInjector<SerializationException> deserializationErrorInjector;

    public void setDeserializationErrorInjector(ErrorInjector<SerializationException> injector) {
        this.deserializationErrorInjector = injector;
    }

    @Override
    public TestLogItem deserialize(InputStream input) throws SerializationException {
        ErrorInjector<SerializationException> errorInjector = this.deserializationErrorInjector;
        if (errorInjector != null) {
            errorInjector.throwIfNecessary();
        }

        return new TestLogItem(input);
    }
}
