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
package io.pravega.controller.task;

/**
 * Task Annotation missing exception.
 */
public class TaskAnnotationNotFoundException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private static final String FORMAT_STRING = "Task annotation missing for method %s.";

    /**
     * Creates a new instance of TaskAnnotationNotFoundException class.
     *
     * @param method method name
     */
    public TaskAnnotationNotFoundException(final String method) {
        super(String.format(FORMAT_STRING, method));
    }

    /**
     * Creates a new instance of TaskAnnotationNotFoundException class.
     *
     * @param method method name
     * @param cause  error cause
     */
    public TaskAnnotationNotFoundException(final String method, final Throwable cause) {
        super(String.format(FORMAT_STRING, method), cause);
    }
}
