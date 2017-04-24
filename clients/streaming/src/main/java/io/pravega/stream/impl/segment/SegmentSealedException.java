/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.stream.impl.segment;

import java.io.IOException;

/**
 * A segment has been sealed and no more events may be written to it.
 */
public class SegmentSealedException extends IOException {

    private static final long serialVersionUID = 1L;

}
