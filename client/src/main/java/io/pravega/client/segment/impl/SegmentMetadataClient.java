package io.pravega.client.segment.impl;

public interface SegmentMetadataClient {
    
    /**
     * Returns the length of the current segment. i.e. the total length of all data written to the segment.
     *
     * @return The length of the current segment.
     */
    abstract long fetchCurrentStreamLength();

    /**
     * Gets the current value of the provided attribute.
     * @param attribute The attribute to get the value of.
     * @return The value of the attribute or {@link SegmentAttribute#NULL_VALUE} if it is not set.
     */
    abstract long getProperty(SegmentAttribute attribute);

    /**
     * Atomically replaces the value of attribute with newValue if it is expectedValue
     * 
     * @param attribute The attribute to set
     * @param expectedValue The value the attribute is expected to be
     * @param newValue The new value for the attribute
     * @return If the replacement occurred. (False if the attribute was not expectedValue)
     */
    abstract boolean compareAndSetAttribute(SegmentAttribute attribute, long expectedValue, long newValue);
    
}
