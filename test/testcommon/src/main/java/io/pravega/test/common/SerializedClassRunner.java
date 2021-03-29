/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.test.common;

import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;

/**
 * This class provides an implementation for the RunWith annotation.
 * It works like the normal BlockJUnit4ClassRunner except that it prohibits 
 * parallelism. So that no two classes with this annotation may run together.
 * This is most useful for tests that rely on metrics.
 */
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
