/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.client.stream.impl.segment;

import java.io.IOException;

/**
 * A segment has ended. No more events may be read from it.
 */
public class EndOfSegmentException extends IOException {

    private static final long serialVersionUID = 1L;

}
