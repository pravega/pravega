/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.hdfs;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.service.storage.SegmentHandle;
import java.util.List;
import lombok.Getter;
import org.apache.http.annotation.GuardedBy;
import org.apache.http.annotation.ThreadSafe;

/**
 * Base Handle for HDFSStorage.
 */
@ThreadSafe
class HDFSSegmentHandle implements SegmentHandle {
    @Getter
    private final String segmentName;
    @Getter
    private final boolean readOnly;
    @GuardedBy("files")
    @Getter
    private final List<FileInfo> files;

    //region Constructor

    /**
     * Creates a new instance of the HDFSSegmentHandle class.
     *
     * @param segmentName The name of the Segment in this Handle, as perceived by users of the Segment interface.
     * @param files       A List of initial files for this handle.
     */
    private HDFSSegmentHandle(String segmentName, boolean readOnly, List<FileInfo> files) {
        Exceptions.checkNotNullOrEmpty(segmentName, "segmentName");
        Exceptions.checkNotNullOrEmpty(files, "files");

        this.segmentName = segmentName;
        this.readOnly = readOnly;
        this.files = files;
    }

    static HDFSSegmentHandle write(String segmentName, List<FileInfo> files) {
        return new HDFSSegmentHandle(segmentName, false, files);
    }

    static HDFSSegmentHandle read(String segmentName, List<FileInfo> files) {
        return new HDFSSegmentHandle(segmentName, true, files);
    }

    //endregion

    void update(List<FileInfo> files) {
        synchronized (this.files) {
            this.files.clear();
            this.files.addAll(files);
        }
    }

    FileInfo getLastFile() {
        synchronized (this.files) {
            return this.files.get(this.files.size() - 1);
        }
    }

    @Override
    public String toString() {
        return String.format("%s (%s)", this.segmentName, this.readOnly ? "R" : "RW");
    }
}
