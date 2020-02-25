/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.tables;

/**
 * Exception that is thrown whenever a conditional {@link KeyValueTable} update failed due to the key not existing.
 * This is different from {@link BadKeyVersionException}.
 */
public class NoSuchKeyException extends ConditionalTableUpdateException {
    /**
     * Creates a new instance of the {@link NoSuchKeyException} class.
     *
     * @param tableName The name of the {@link KeyValueTable} affected.
     */
    public NoSuchKeyException(String tableName) {
        super(tableName);
    }
}