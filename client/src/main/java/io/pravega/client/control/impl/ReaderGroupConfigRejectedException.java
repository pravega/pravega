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
package io.pravega.client.control.impl;

/**
 * The {@link io.pravega.client.stream.ReaderGroupConfig} sent to be updated on the controller is invalid.
 * This is likely due to the {@link io.pravega.client.stream.ReaderGroupConfig#generation} being invalid.
 */
public class ReaderGroupConfigRejectedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public ReaderGroupConfigRejectedException(String s) {
        super(s);
    }
}
