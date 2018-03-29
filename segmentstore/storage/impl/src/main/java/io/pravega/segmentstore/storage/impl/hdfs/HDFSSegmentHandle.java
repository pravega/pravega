/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.hdfs;

import io.pravega.common.Exceptions;
import io.pravega.segmentstore.storage.SegmentHandle;
import lombok.Getter;
import org.apache.http.annotation.GuardedBy;
import org.apache.http.annotation.ThreadSafe;

/**
 * Base Handle for HDFSStorage.
 */
@ThreadSafe
class HDFSSegmentHandle implements SegmentHandle {
    //region Members

    @Getter
    private final String segmentName;
    @Getter
    private final boolean readOnly;
    @GuardedBy("files")
    @Getter
    private final String file;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the HDFSSegmentHandle class.
     *  @param segmentName The name of the Segment in this Handle, as perceived by users of the Storage interface.
     * @param readOnly    Whether this handle is read-only or not.
     * @param file       The HDFS file for this handle.
     */
    private HDFSSegmentHandle(String segmentName, boolean readOnly, String file) {
        this.segmentName = Exceptions.checkNotNullOrEmpty(segmentName, "segmentName");
        this.readOnly = readOnly;
        this.file = Exceptions.checkNotNullOrEmpty(file, "file");
    }

    /**
     * Creates a read-write handle.
     *
     * @param segmentName The name of the Segment to create the handle for.
     * @param file       The HDFS file for this handle.
     * @return The new handle.
     */
    static HDFSSegmentHandle write(String segmentName, String file) {
        return new HDFSSegmentHandle(segmentName, false, file);
    }

    /**
     * Creates a read-only handle.
     *
     * @param segmentName The name of the Segment to create the handle for.
     * @param file       The HDFS file for this handle.
     * @return The new handle.
     */
    static HDFSSegmentHandle read(String segmentName, String file) {
        return new HDFSSegmentHandle(segmentName, true, file);
    }

    //endregion

    //region Properties

    @Override
    public String toString() {
        return String.format("[%s] %s", this.readOnly ? "R" : "RW", this.segmentName);
    }

    //endregion
}
