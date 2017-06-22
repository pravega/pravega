/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.filesystem;

import io.pravega.common.util.Retry;
import java.util.function.Predicate;
import java.util.function.Supplier;
import lombok.SneakyThrows;

/**
 * Helper class to retry a given operation.
 */
public class FileSystemRetryHelper {
    /**
     *  Static helper function to retry given operation retryCount times based on condition retryCheck.
     * @param supplier          The function to retry.
     * @param retryCheck         The function is retried till the time this condition remains true.
     * @param errorHandler      This function is called to get relevant exception if the retries exceed.
     * @param retryCount        Number of times the function is retried.
     * @param <T>               Return parameter of the function.
     * @return                  Return value of an successful attempt.
     *                          Exception supplied by errorHandler is thrown in case the retries exceed retryCount.
     */
    @SneakyThrows
    public static <T> T retry(Retry.Retryable<T, Exception, Exception> supplier,
                             Predicate<T> retryCheck, Supplier<Exception> errorHandler, int retryCount) {
        T retVal;
        do {
            retVal = supplier.attempt();
            if (!retryCheck.test(retVal)) {
                return retVal;
            }
        } while (--retryCount != 0);
        throw errorHandler.get();
    }
}
