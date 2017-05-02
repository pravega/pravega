/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.server.segmentstore.contracts;

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
     */
    public int getLength() {
        return this.length;
    }

    /**
     * Gets an InputStream representing the Data that was retrieved.
     */
    public InputStream getData() {
        return this.data;
    }

    @Override
    public String toString() {
        return String.format("Length = %d", getLength());
    }
}
