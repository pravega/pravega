/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.contracts.tables;

/**
 * Exception that is thrown whenever a Table Key exceeds the maximum allowed length.
 */
public class TableKeyTooLongException extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of the TableKeyTooLongException class.
     *
     * @param key           The Key.
     * @param maximumLength The maximum allowed key length.
     */
    public TableKeyTooLongException(TableKey key, int maximumLength) {
        super(String.format("Table Key too long. Maximum length: %s, given: %s.", key.getKey().getLength(), maximumLength));
    }
}
