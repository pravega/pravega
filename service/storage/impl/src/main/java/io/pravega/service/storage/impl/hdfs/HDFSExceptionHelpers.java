/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pravega.service.storage.impl.hdfs;

import io.pravega.service.contracts.StreamSegmentExistsException;
import io.pravega.service.contracts.StreamSegmentNotExistsException;
import io.pravega.service.contracts.StreamSegmentSealedException;
import java.io.FileNotFoundException;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.ipc.RemoteException;

/**
 * Helps translate HDFS specific Exceptions to Pravega-equivalent Exceptions.
 */
final class HDFSExceptionHelpers {

    /**
     * Translates HDFS specific Exceptions to Pravega-equivalent Exceptions.
     *
     * @param segmentName Name of the stream segment on which the exception occurs.
     * @param e           The exception to be translated.
     * @return The translated exception.
     */
    static Throwable translateFromException(String segmentName, Throwable e) {
        Throwable retVal = e;

        if (e instanceof RemoteException) {
            retVal = e = ((RemoteException) e).unwrapRemoteException();
        }

        if (e instanceof PathNotFoundException || e instanceof FileNotFoundException) {
            retVal = new StreamSegmentNotExistsException(segmentName, e);
        }

        if (e instanceof FileAlreadyExistsException) {
            retVal = new StreamSegmentExistsException(segmentName, e);
        }

        if (e instanceof AclException) {
            retVal = new StreamSegmentSealedException(segmentName, e);
        }

        return retVal;
    }

    /**
     * Returns a new instance of the HDFS equivalent of StreamSegmentExistsException.
     *
     * @param segmentName The name of the segment to construct the Exception for.
     * @return The new exception.
     */
    static FileAlreadyExistsException segmentExistsException(String segmentName) {
        return new FileAlreadyExistsException(segmentName);
    }

    /**
     * Returns a new instance of the HDFS equivalent of StreamSegmentNotExistsException.
     *
     * @param segmentName The name of the segment to construct the Exception for.
     * @return The new exception.
     */
    static FileNotFoundException segmentNotExistsException(String segmentName) {
        return new FileNotFoundException(segmentName);
    }

    /**
     * Returns a new instance of the HDFS equivalent of StreamSegmentSealedException.
     *
     * @param segmentName The name of the segment to construct the Exception for.
     * @return The new exception.
     */
    static AclException segmentSealedException(String segmentName) {
        return new AclException(segmentName);
    }
}
