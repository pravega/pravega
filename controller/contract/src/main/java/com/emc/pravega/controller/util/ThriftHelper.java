/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.controller.util;

import org.apache.thrift.TException;

/**
 * Thrift helper functions
 */
public final class ThriftHelper {

    @FunctionalInterface
    public static interface ThriftCall<ResultT> {
        ResultT call() throws TException;
    }

    /**
     * Eliminates boilerplate code of wrapping 'checked' thrift exception into a RuntimeException.
     *
     * @param call thrift method call
     * @return result of the call
     */
    public static <ResultT> ResultT thriftCall(ThriftCall<ResultT> call) {
        try {
            return call.call();
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }
}
