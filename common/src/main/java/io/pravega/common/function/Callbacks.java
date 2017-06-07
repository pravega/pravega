/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.function;

/**
 * Misc methods that can be used as callbacks.
 */
public final class Callbacks {
    /**
     * Empty consumer. Does nothing.
     *
     * @param ignored Ignored argument.
     * @param <T>     Return type. Ignored.
     */
    public static <T> void doNothing(T ignored) {
        // This method intentionally left blank.
    }
}
