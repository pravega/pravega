package com.emc.logservice.server.reading;

/**
 * Read Index Entry that redirects to a different StreamSegment.
 */
public class RedirectReadIndexEntry extends ReadIndexEntry {
    private final StreamSegmentReadIndex redirectReadIndex;

    /**
     * Creates a new instance of the RedirectReadIndexEntry class.
     *
     * @param streamSegmentOffset The StreamSegment offset for this entry.
     * @param length              The length of the redirected Read Index.
     * @param redirectReadIndex   The StreamSegmentReadIndex to redirect to.
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException if the offset is a negative number.
     */
    protected RedirectReadIndexEntry(long streamSegmentOffset, long length, StreamSegmentReadIndex redirectReadIndex) {
        super(streamSegmentOffset, length);

        if (redirectReadIndex == null) {
            throw new NullPointerException("redirectReadIndex");
        }

        this.redirectReadIndex = redirectReadIndex;
    }

    protected StreamSegmentReadIndex getRedirectReadIndex() {
        return this.redirectReadIndex;
    }
}
