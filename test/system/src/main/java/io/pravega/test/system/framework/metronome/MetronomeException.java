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
package io.pravega.test.system.framework.metronome;

/**
 * Exceptions while accessing Metronome.
 */
public class MetronomeException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private int status;
    private String message;

    public MetronomeException(int status, String message) {
        this.status = status;
        this.message = message;
    }

    /**
     * Gets the HTTP status code of the failure, such as 404.
     * @return  status code.
     */
    public int getStatus() {
        return status;
    }

    @Override
    public String getMessage() {
        return message + " (http status: " + status + ")";
    }

    @Override
    public String toString() {
        return message + " (http status: " + status + ")";
    }
}
