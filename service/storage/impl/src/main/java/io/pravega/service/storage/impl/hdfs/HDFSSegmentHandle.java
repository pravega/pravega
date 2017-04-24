/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package io.pravega.service.storage.impl.hdfs;

import io.pravega.common.Exceptions;
import io.pravega.service.storage.SegmentHandle;
import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.List;
import lombok.Getter;
import org.apache.commons.lang.StringUtils;
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
    private final List<FileDescriptor> files;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the HDFSSegmentHandle class.
     *
     * @param segmentName The name of the Segment in this Handle, as perceived by users of the Segment interface.
     * @param readOnly    Whether this handle is read-only or not.
     * @param files       A ordered list of initial files for this handle.
     */
    private HDFSSegmentHandle(String segmentName, boolean readOnly, List<FileDescriptor> files) {
        Exceptions.checkNotNullOrEmpty(segmentName, "segmentName");
        Exceptions.checkNotNullOrEmpty(files, "files");

        this.segmentName = segmentName;
        this.readOnly = readOnly;
        this.files = files;
    }

    /**
     * Creates a read-write handle.
     *
     * @param segmentName The name of the Segment to create the handle for.
     * @param files       A ordered list of initial files for this handle.
     * @return The new handle.
     */
    static HDFSSegmentHandle write(String segmentName, List<FileDescriptor> files) {
        return new HDFSSegmentHandle(segmentName, false, files);
    }

    /**
     * Creates a read-only handle.
     *
     * @param segmentName The name of the Segment to create the handle for.
     * @param files       A ordered list of initial files for this handle.
     * @return The new handle.
     */
    static HDFSSegmentHandle read(String segmentName, List<FileDescriptor> files) {
        return new HDFSSegmentHandle(segmentName, true, files);
    }

    //endregion

    //region Properties

    /**
     * Gets the last file in the file list for this handle.
     */
    FileDescriptor getLastFile() {
        synchronized (this.files) {
            return this.files.get(this.files.size() - 1);
        }
    }

    /**
     * Replaces the files in this handle with the given ones.
     *
     * @param newFiles The new files to replace with.
     */
    void replaceFiles(Collection<FileDescriptor> newFiles) {
        synchronized (this.files) {
            this.files.clear();
            this.files.addAll(newFiles);
        }
    }

    /**
     * Replaces the last file with the given one.
     *
     * @param fileDescriptor The file to replace with.
     */
    void replaceLastFile(FileDescriptor fileDescriptor) {
        synchronized (this.files) {
            Preconditions.checkState(this.files.size() > 0, "Insufficient number of files in the handle to perform this operation.");
            int lastIndex = this.files.size() - 1;
            FileDescriptor lastFile = this.files.get(lastIndex);

            long expectedOffset = lastFile.getOffset();
            Preconditions.checkArgument(fileDescriptor.getOffset() == expectedOffset,
                    "Invalid offset. Expected %s, actual %s.", expectedOffset, fileDescriptor.getOffset());

            long expectedMinEpoch = lastFile.getEpoch();
            Preconditions.checkArgument(fileDescriptor.getEpoch() >= expectedMinEpoch,
                    "Invalid epoch. Expected at least %s, actual %s.", expectedMinEpoch, fileDescriptor.getEpoch());

            this.files.remove(lastIndex);
            this.files.add(fileDescriptor);
        }
    }

    /**
     * Removes the last file from this handle.
     */
    void removeLastFile() {
        synchronized (this.files) {
            // Need at least one file in the handle at any given time.
            Preconditions.checkState(this.files.size() > 1, "Insufficient number of files in the handle to perform this operation.");
            this.files.remove(this.files.size() - 1);
        }
    }

    /**
     * Adds a new file at the end of this handle.
     *
     * @param fileDescriptor The FileDescriptor of the file to add.
     */
    void addLastFile(FileDescriptor fileDescriptor) {
        synchronized (this.files) {
            FileDescriptor lastFile = getLastFile();
            Preconditions.checkState(lastFile.isReadOnly(), "Cannot add a new file if the current last file is not read-only.");

            long expectedOffset = lastFile.getLastOffset();
            Preconditions.checkArgument(fileDescriptor.getOffset() == expectedOffset,
                    "Invalid offset. Expected %s, actual %s.", expectedOffset, fileDescriptor.getOffset());

            long expectedMinEpoch = lastFile.getEpoch();
            Preconditions.checkArgument(fileDescriptor.getEpoch() >= expectedMinEpoch,
                    "Invalid epoch. Expected at least %s, actual %s.", expectedMinEpoch, fileDescriptor.getEpoch());

            this.files.add(fileDescriptor);
        }
    }

    @Override
    public String toString() {
        String fileNames;
        synchronized (this.files) {
            fileNames = StringUtils.join(this.files, ", ");
        }

        return String.format("[%s] %s (Files: %s)", this.readOnly ? "R" : "RW", this.segmentName, fileNames);
    }

    //endregion
}
