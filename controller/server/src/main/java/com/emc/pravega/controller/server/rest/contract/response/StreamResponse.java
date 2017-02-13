/**
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.controller.server.rest.contract.response;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * REST representation of response object of a stream.
 * Stream properties wrapped in stream response.
 */
@Getter
@Setter
@AllArgsConstructor
public class StreamResponse implements Serializable {
    private StreamProperty stream;
}
