/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.hdfs;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.service.storage.SegmentHandle;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.commons.lang.NotImplementedException;

/**
 * SegmentHandle for HDFSStorage.
 */
@EqualsAndHashCode(of = {"segmentName", "readOnly"})
class HDFSSegmentHandle implements SegmentHandle {
    @Getter
    private final String segmentName;
    @Getter
    private final boolean readOnly;

    //region Constructor

    /**
     * Creates a new instance of the HDFSSegmentHandle class.
     *
     * @param segmentName The name of the Segment in this Handle, as perceived by users of the Segment interface.
     * @param readOnly    If true, this handle can only be used for non-modify operations.
     */
    HDFSSegmentHandle(String segmentName, boolean readOnly) {
        Exceptions.checkNotNullOrEmpty(segmentName, "segmentName");
        this.segmentName = segmentName;
        this.readOnly = readOnly;
    }

    //endregion

    @Override
    public String toString() {
        return String.format("(%s) %s", this.readOnly ? "R" : "RW", this.segmentName);
    }

    //region Not yet implemented methods. TODO: implement these in the next phase.

    /**
     * Gets a value indicating whether the Segment is sealed for modifications.
     * Note that isReadOnly() == true and isSealed() == true is a valid combination; in this case we were able to open
     * a ReadWrite handle, but it is up to the Storage adapter to enforce Seal constraints.
     */
    public boolean isSealed() {
        throw new NotImplementedException();
    }

    /**
     * Gets a value indicating the Epoch of this handle. An Epoch is a monotonically strict increasing number that changes
     * every time the owning SegmentContainer has been successfully recovered. The monotonicity of this number can be used
     * as a proxy to determine the Container that should currently own the Segment should there be situations that
     * require conflict resolution. See HDFSStorage documentation for more details.
     */
    public long getEpoch() {
        throw new NotImplementedException();
    }

    //endregion
}
