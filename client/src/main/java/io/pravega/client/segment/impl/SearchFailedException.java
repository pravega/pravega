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
package io.pravega.client.segment.impl;

/**
 * Exception that is thrown when unable to locate nextOffset for the segment.
 */
public class SearchFailedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public SearchFailedException(String segment) {
        super("Search failed while getting segment next offset: " + segment);
    }

    public SearchFailedException(String segment, Throwable t) {
        super("Search failed while getting segment next offset: " + segment, t);
    }

}