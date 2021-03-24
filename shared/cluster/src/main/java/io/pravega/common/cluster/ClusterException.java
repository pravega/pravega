/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
