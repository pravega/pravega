/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.cluster;

public class ClusterException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public enum Type {
        METASTORE
    }

    final String message;

    public ClusterException(String message) {
        this.message = message;
    }

    /**
     * Factory method to construct Store exceptions.
     *
     * @param type Type of Exception.
     * @param message Exception message
     * @return Instance of ClusterException.
     */
    public static ClusterException create(Type type, String message) {
        switch (type) {
            case METASTORE:
                return new MetaStoreException(message);
            default:
                throw new IllegalArgumentException("Invalid exception type");
        }
    }

    public static class MetaStoreException extends ClusterException {
        private static final long serialVersionUID = 1L;

        public MetaStoreException(String message) {
            super(message);
        }
    }

}
