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
