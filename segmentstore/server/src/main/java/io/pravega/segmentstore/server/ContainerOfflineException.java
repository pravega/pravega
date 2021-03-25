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
package io.pravega.segmentstore.server;

/**
 * Exception thrown whenever a Container is offline.
 */
public class ContainerOfflineException extends IllegalContainerStateException {
    private static final long serialVersionUID = 1L;

    /**
     * Creates an new instance of the ContainerOfflineException class.
     *
     * @param containerId The Id of the SegmentContainer that is offline.
     */
    public ContainerOfflineException(int containerId) {
        super(String.format("Container %d is offline and cannot execute any operation.", containerId));
    }
}
