/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server.logs;

import com.emc.pravega.service.server.LogItemFactory;
import com.emc.pravega.testcommon.ErrorInjector;

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
