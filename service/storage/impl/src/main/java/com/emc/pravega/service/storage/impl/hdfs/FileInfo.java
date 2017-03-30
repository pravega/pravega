/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.hdfs;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Created by andrei on 3/29/17.
 */
@AllArgsConstructor
public class FileInfo {
    @Getter
    private final String path;
    @Getter
    private final long offset;
    @Getter
    private final long length;
    @Getter
    private final long epoch;
    @Getter
    private boolean readOnly;

    void markReadOnly() {
        this.readOnly = true;
    }

    @Override
    public String toString() {
        return String.format("%s (%d, %s)", this.path, this.length, this.readOnly ? "R" : "RW");
    }
}