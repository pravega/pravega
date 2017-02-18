/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.stream;

/**
 * A transaction has failed. Usually because of it timed out or someone called
 * {@link Transaction#abort()}
 */
public class TxnFailedException extends Exception {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of TxnFailedException class.
     */
    public TxnFailedException() {
        super();
    }

    /**
     * Creates a new instance of TxnFailedException class.
     *
     * @param e The cause.
     */
    public TxnFailedException(Throwable e) {
        super(e);
    }

    /**
     * Creates a new instance of TxnFailedException class.
     *
     * @param msg Exception description.
     * @param e   The cause.
     */
    public TxnFailedException(String msg, Throwable e) {
        super(msg, e);
    }

    /**
     * Creates a new instance of TxnFailedException class.
     *
     * @param msg Exception description.
     */
    public TxnFailedException(String msg) {
        super(msg);
    }
}
