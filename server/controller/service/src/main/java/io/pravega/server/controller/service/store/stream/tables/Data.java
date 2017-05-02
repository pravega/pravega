/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.server.controller.service.store.stream.tables;

@lombok.Data
public class Data<T> {
    private final byte[] data;
    private final T version;
}
