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
package io.pravega.controller.store.stream;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.controller.retryable.RetryableException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * StoreException is a high level exception thrown when an exception arises from Stream Store.
 */
@Slf4j
@Getter
public class StoreException extends RuntimeException {

    /**
     * Enum to describe the type of exception.
     */
    public enum Type {
        DATA_EXISTS,
        DATA_NOT_FOUND,
        DATA_CONTAINER_NOT_FOUND,
        DATA_CONTAINS_ELEMENTS,
        WRITE_CONFLICT,
        ILLEGAL_STATE,
        OPERATION_NOT_ALLOWED,
        CONNECTION_ERROR,
        UNKNOWN
    }

    /**
     * Construct a StoreException.
     *
     * @param errorMessage  The detailed error message.
     * @param cause         Exception cause.
     */
    private StoreException(final String errorMessage, final Throwable cause) {
        super(errorMessage, cause);
    }

    /**
     * Factory method to construct Store exceptions.
     *
     * @param type  Type of Exception.
     * @param cause Exception cause.
     * @return Instance of StoreException.
     */
    public static StoreException create(final Type type, final Throwable cause) {
        Preconditions.checkNotNull(cause, "cause");
        return create(type, cause, null);
    }

    /**
     * Factory method to construct Store exceptions.
     *
     * @param type          Type of Exception.
     * @param errorMessage  The detailed error message.
     * @return Instance of StoreException.
     */
    public static StoreException create(final Type type, final String errorMessage) {
        Exceptions.checkNotNullOrEmpty(errorMessage, "errorMessage");
        return create(type, null, errorMessage);
    }

    /**
     * Factory method to construct Store exceptions.
     *
     * @param type          Type of Exception.
     * @param cause         Exception cause.
     * @param errorMessage  The detailed error message.
     * @return Instance of type of StoreException.
     */
    public static StoreException create(final Type type, final Throwable cause, final String errorMessage) {
        Preconditions.checkArgument(cause != null || (errorMessage != null && !errorMessage.isEmpty()),
                "Either cause or errorMessage should be non-empty");
        StoreException exception;
        switch (type) {
            case DATA_EXISTS:
                exception = new DataExistsException(errorMessage, cause);
                break;
            case DATA_NOT_FOUND:
                exception = new DataNotFoundException(errorMessage, cause);
                break;
            case DATA_CONTAINER_NOT_FOUND:
                exception = new DataContainerNotFoundException(errorMessage, cause);
                break;
            case DATA_CONTAINS_ELEMENTS:
                exception = new DataNotEmptyException(errorMessage, cause);
                break;
            case WRITE_CONFLICT:
                exception = new WriteConflictException(errorMessage, cause);
                break;
            case ILLEGAL_STATE:
                exception = new IllegalStateException(errorMessage, cause);
                break;
            case OPERATION_NOT_ALLOWED:
                exception = new OperationNotAllowedException(errorMessage, cause);
                break;
            case CONNECTION_ERROR:
                exception = new StoreConnectionException(errorMessage, cause);
                break;
            case UNKNOWN:
                exception = new UnknownException(errorMessage, cause);
                break;
            default:
                throw new IllegalArgumentException("Invalid exception type");
        }
        return exception;
    }

    /**
     * Exception type when node exists, and duplicate node is created.
     */
    public static class DataExistsException extends StoreException {
        private DataExistsException(String errorMessage, Throwable cause) {
            super(errorMessage, cause);
        }
    }

    /**
     * Exception type when data does not exist and is operated on.
     */
    public static class DataNotFoundException extends StoreException {
        private DataNotFoundException(String errorMessage, Throwable cause) {
            super(errorMessage, cause);
        }
    }

    /**
     * Exception type when the parent container of the data does not exist.
     */
    public static class DataContainerNotFoundException extends DataNotFoundException {
        private DataContainerNotFoundException(String errorMessage, Throwable cause) {
            super(errorMessage, cause);
        }
    }

    /**
     * Exception type when deleting a non empty node.
     */
    public static class DataNotEmptyException extends StoreException {
        private DataNotEmptyException(String errorMessage, Throwable cause) {
            super(errorMessage, cause);
        }
    }

    /**
     * Exception type when you are attempting to update a stale value.
     */
    public static class WriteConflictException extends StoreException implements RetryableException {
        private WriteConflictException(String errorMessage, Throwable cause) {
            super(errorMessage, cause);
        }
    }

    /**
     * Exception type when you are attempting a disallowed operation.
     */
    public static class IllegalStateException extends StoreException {
        private IllegalStateException(String errorMessage, Throwable cause) {
            super(errorMessage, cause);
        }
    }

    /**
     * Exception type when the attempted operation is currently not allowed.
     */
    public static class OperationNotAllowedException extends StoreException {
        private OperationNotAllowedException(String errorMessage, Throwable cause) {
            super(errorMessage, cause);
        }
    }

    /**
     * Exception type due to failure in connecting to the store.
     */
    public static class StoreConnectionException extends StoreException implements RetryableException {
        private StoreConnectionException(String errorMessage, Throwable cause) {
            super(errorMessage, cause);
        }
    }

    /**
     * Exception type when the cause is not known.
     */
    public static class UnknownException extends StoreException {
        private UnknownException(String errorMessage, Throwable cause) {
            super(errorMessage, cause);
        }
    }
}
