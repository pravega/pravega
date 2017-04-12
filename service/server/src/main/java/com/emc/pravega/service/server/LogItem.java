/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server;

import com.emc.pravega.shared.common.util.SequencedItemList;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Defines the minimum structure of an Item that is appended to a Sequential Log.
 */
public interface LogItem extends SequencedItemList.Element {
    /**
     * Serializes this item to the given OutputStream.
     *
     * @param output The OutputStream to serialize to.
     * @throws IOException           If the given OutputStream threw one.
     * @throws IllegalStateException If the serialization conditions are not met.
     */
    void serialize(OutputStream output) throws IOException;
}
