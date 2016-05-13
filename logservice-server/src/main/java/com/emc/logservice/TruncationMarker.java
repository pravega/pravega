package com.emc.logservice;

/**
 * A marker that can be used for mapping valid truncation points from Log Operations to Data Frames.
 */
public class TruncationMarker
{
    private final long operationSequenceNumber;
    private final long dataFrameSequenceNumber;

    /**
     * Creates a new instance of the TruncationMarker class.
     * @param operationSequenceNumber The Sequence Number of the Operation that can be used as a truncation argument.
     * @param dataFrameSequenceNumber The Sequence Number of the corresponding Data Frame that can be truncated (up to, and including).
     */
    public TruncationMarker(long operationSequenceNumber, long dataFrameSequenceNumber)
    {
        this.operationSequenceNumber = operationSequenceNumber;
        this.dataFrameSequenceNumber = dataFrameSequenceNumber;
    }

    /**
     * Gets a value indicating the Sequence Number of the Operation that can be used as a Truncation argument.
     * @return
     */
    public long getOperationSequenceNumber()
    {
        return this.operationSequenceNumber;
    }

    /**
     * Gets a value indicating the Sequence Number of the corresponding Data Frame that can be truncated (up to, and including).
     * @return
     */
    public long getDataFrameSequenceNumber()
    {
        return this.dataFrameSequenceNumber;
    }

    @Override
    public String toString()
    {
        return String.format("Operation.SeqNo = %d, DataFrame.SeqNo = %d", getOperationSequenceNumber(), getDataFrameSequenceNumber());
    }
}
