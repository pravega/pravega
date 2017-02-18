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
package com.emc.pravega.state.impl;

public class CorruptedStateException extends RuntimeException {

    /**
     * Constructor for corrupted state exception with message.
     *
     * @param message Exception description
     */
    public CorruptedStateException(String message) {
        super(message);
    }

    /**
     * Constructor for corrupted state exception with message and exception.
     *
     * @param string Exception message
     * @param e      The exception instance indicating a corrupt state and on which the new exception will build on.
     */
    public CorruptedStateException(String string, Exception e) {
        super(string, e);
    }

}
