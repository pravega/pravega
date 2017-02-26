/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.util;

import org.apache.thrift.TException;

/**
 * Thrift helper functions.
 */
public final class ThriftHelper {

    @FunctionalInterface
    public static interface ThriftCall<ResultT> {
        ResultT call() throws TException;
    }

    /**
     * Eliminates boilerplate code of wrapping 'checked' thrift exception into a RuntimeException.
     *
     * @param <ResultT> The type of the result of the call
     * @param call thrift method call
     */
    public static <ResultT> ResultT thriftCall(ThriftCall<ResultT> call) {
        try {
            return call.call();
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }
}
