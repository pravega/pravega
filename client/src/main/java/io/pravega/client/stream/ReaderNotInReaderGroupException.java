/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream;

import lombok.Getter;

/**
 * Read was invoked on a reader that the reader group does not consider a member.
 * This likely means the reader was declared dead by invoking {@link ReaderGroup#readerOffline(String, Position)}
 */
public class ReaderNotInReaderGroupException extends Exception {

    private static final long serialVersionUID = 1L;

    @Getter
    private final String readerId;
 
    public ReaderNotInReaderGroupException(String readerId) {
        super("Reader: " + readerId);
        this.readerId = readerId;
    }
}
