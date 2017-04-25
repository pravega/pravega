/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
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
