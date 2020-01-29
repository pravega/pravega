/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.contracts;

import java.io.InputStream;

/**
 * Contents for a ReadResultEntry.
 */
public class ReadResultEntryContents {
    private final int length;
    private final InputStream data;

    /**
     * Creates a new instance of the ReadResultEntryContents class.
     *
     * @param data                The data to retrieve.
     * @param length              The length of the retrieved data.
     */
    public ReadResultEntryContents(InputStream data, int length) {
        this.data = data;
        this.length = length;
    }

    /**
     * Gets a value indicating the length of the Data Stream.
     *
     * @return length of the data stream
     *
     */
    public int getLength() {
        return this.length;
    }

    /**
     * Gets an InputStream representing the Data that was retrieved.
     *
     * @return Stream representing retrieved data
     *
     */
    public InputStream getData() {
        return this.data;
    }

    @Override
    public String toString() {
        return String.format("Length = %d", getLength());
    }
}
