/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.contracts;

/**
 * Exception that is thrown whenever an Append Operation failed because of bad Attribute Updates.
 */
public class BadAttributeUpdateException extends StreamSegmentException {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of the BadAttributeUpdateException class.
     *
     * @param streamSegmentName The name of the StreamSegment.
     * @param attributeUpdate   The AttributeUpdate that failed the check.
     * @param errorMessage      The Event Number that was given as part of the Operation.
     */
    public BadAttributeUpdateException(String streamSegmentName, AttributeUpdate attributeUpdate, String errorMessage) {
        super(streamSegmentName, getMessage(attributeUpdate, errorMessage));
    }

    private static String getMessage(AttributeUpdate attributeUpdate, String errorMessage) {
        return String.format("Bad Attribute Update (%s): %s.", attributeUpdate, errorMessage);
    }
}
