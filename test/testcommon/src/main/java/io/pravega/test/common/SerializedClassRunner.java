/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.common;

import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;

public class SerializedClassRunner extends BlockJUnit4ClassRunner {

    private static final Object LOCK = new Object(); 
    
    public SerializedClassRunner(Class<?> klass) throws InitializationError {
        super(klass);
    }
    
    @Override
    public void run(RunNotifier notifier) {
        synchronized (LOCK) {            
            super.run(notifier);
        }
    }

}
