/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.controller.store.stream;

import lombok.Data;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * List of segments from where a consumer can start reading, or a producer can start producing to.
 * For each segment, SegmentFutures also lists all the segments that can be read from (produced to) after
 * that segment is completely read (sealed).
 */
@Data
public class SegmentFutures {
    // current segments to read from or write to
    private final List<Integer> current;

    // future segments to read from or write to mapped to the current segment it follows when it is completely read (consumer) or sealed (producer) 
    private final Map<Integer, Integer> futures;

    public SegmentFutures(final List<Integer> current, final Map<Integer, Integer> futures) {
        this.current = Collections.unmodifiableList(current);
        this.futures = Collections.unmodifiableMap(futures);
    }
}
