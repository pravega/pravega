/**
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.service.server;

import com.emc.pravega.service.contracts.StreamingException;

/**
 * Exception that is thrown whenever we detect an unrecoverable data corruption.
 * Usually, after this is thrown, our only resolution may be to suspend processing in the container or completely bring it offline.
 */
public class DataCorruptionException extends StreamingException {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public DataCorruptionException(String message) {
        super(message);
    }

    public DataCorruptionException(String message, Throwable cause) {
        super(message, cause);
    }

    public DataCorruptionException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
