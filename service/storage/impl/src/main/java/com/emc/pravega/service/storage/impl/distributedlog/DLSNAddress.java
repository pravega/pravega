/**
 *  Copyright (c) 2017 Dell Inc. or its subsidiaries. All Rights Reserved
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
package com.emc.pravega.service.storage.impl.distributedlog;

import com.emc.pravega.service.storage.LogAddress;
import com.google.common.base.Preconditions;
import com.twitter.distributedlog.DLSN;

/**
 * LogAddress for DistributedLog. Wraps around DistributedLog-specific addressing scheme, using DLSNs, without exposing
 * such information to the outside.
 */
class DLSNAddress extends LogAddress {
    private final DLSN dlsn;

    /**
     * Creates a new instance of the LogAddress class.
     *
     * @param sequence The sequence of the address (location).
     */
    public DLSNAddress(long sequence, DLSN dlsn) {
        super(sequence);
        Preconditions.checkNotNull(dlsn, "dlsn");
        this.dlsn = dlsn;
    }

    /**
     * Gets the DLSN associated with this LogAddress.
     */
    DLSN getDLSN() {
        return this.dlsn;
    }

    @Override
    public String toString() {
        return String.format("%s, DLSN = %s", super.toString(), this.dlsn);
    }
}
