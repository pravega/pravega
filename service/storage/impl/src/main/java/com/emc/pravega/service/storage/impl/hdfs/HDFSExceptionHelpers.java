/**
 *  Copyright (c) 2016 Dell Inc. or its subsidiaries. All Rights Reserved
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
package com.emc.pravega.service.storage.impl.hdfs;

import com.emc.pravega.service.contracts.StreamSegmentExistsException;
import com.emc.pravega.service.contracts.StreamSegmentNotExistsException;
import com.emc.pravega.service.contracts.StreamSegmentSealedException;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.ipc.RemoteException;

import java.io.FileNotFoundException;

/**
 * Helps to translated HDFS specific IOExceptions to StreamSegmentExceptions.
 * */
public class HDFSExceptionHelpers {

    /**
     * API to translated HDFS specific IOExceptions to StreamSegmentExceptions.
     * @param streamSegmentName Name of the stream segment on which the exception occurs.
     * @param e The exception to be translated
     * @return
     */
    public static Exception translateFromException(String streamSegmentName, Exception e) {
        Exception retVal = e;

        if (e instanceof RemoteException) {
            retVal = e = ((RemoteException) e).unwrapRemoteException();
        }

        if (e instanceof PathNotFoundException || e instanceof FileNotFoundException) {
            retVal = new StreamSegmentNotExistsException(streamSegmentName);
        }

        if (e instanceof FileAlreadyExistsException) {
            retVal = new StreamSegmentExistsException(streamSegmentName);
        }

        if (e instanceof AclException) {
            retVal = new StreamSegmentSealedException(streamSegmentName);
        }

        return retVal;

    }
}
