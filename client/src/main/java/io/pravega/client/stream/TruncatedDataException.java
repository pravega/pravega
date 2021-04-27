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
package io.pravega.client.stream;

/**
 * TruncatedDataException is thrown if the data to be read next has been truncated away and can no longer be read.
 */
public class TruncatedDataException extends RuntimeException {

    private static final long serialVersionUID = 1L;
    
    /**
     * Creates a new instance of TruncatedDataException class.
     */
    public TruncatedDataException() {
        super();
    }

    /**
     * Creates a new instance of TruncatedDataException class.
     *
     * @param e The cause.
     */
    public TruncatedDataException(Throwable e) {
        super(e);
    }

    /**
     * Creates a new instance of TruncatedDataException class.
     *
     * @param msg Exception description.
     * @param e   The cause.
     */
    public TruncatedDataException(String msg, Throwable e) {
        super(msg, e);
    }

    /**
     * Creates a new instance of TruncatedDataException class.
     *
     * @param msg Exception description.
     */
    public TruncatedDataException(String msg) {
        super(msg);
    }
}
