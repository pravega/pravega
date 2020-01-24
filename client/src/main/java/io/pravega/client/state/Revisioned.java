/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.state;

/**
 * An object that has a revision associated with it.
 * It is assumed that if two objects have the same streamName, and revision, that they are equal. i.e.
 * a.equals(b) should return true.
 */
public interface Revisioned {
    
    /**
     * Returns the scoped name of this stream used to persist this object.
     *
     * @return String indicating full stream name including its scope appended to it
     */
    String getScopedStreamName();
    
    /**
     * Returns the revision corresponding to this object.
     *
     * @return Revision object
     */
    Revision getRevision();
}
