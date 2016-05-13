package com.emc.logservice.ReadIndex;

/**
 * A ReadIndexEntry that has actual data in memory.
 */
public class ByteArrayReadIndexEntry extends ReadIndexEntry
{
    //region Members

    private final byte[] data;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ReadIndexEntry class.
     *
     * @param streamSegmentOffset The StreamSegment offset for this entry.
     * @param data                The data.
     * @throws NullPointerException     If data is null.
     * @throws IllegalArgumentException if the offset is a negative number.
     */
    protected ByteArrayReadIndexEntry(long streamSegmentOffset, byte[] data)
    {
        super(streamSegmentOffset, data.length);
        this.data = data;
    }

    //endregion

    //region Properties

    /**
     * Gets a byte array containing the data for this entry.
     *
     * @return
     */
    public byte[] getData()
    {
        return this.data;
    }

    //endregion
}
