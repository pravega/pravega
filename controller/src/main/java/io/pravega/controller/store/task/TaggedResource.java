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
package io.pravega.controller.store.task;

import lombok.Data;

/**
 * Resource with a tag.
 * Tag is a logical identifier intended to distinguish between multiple attempts by a single controller host to
 * execute tasks on a resource.
 */
@Data
public class TaggedResource {

    private final String tag;
    private final Resource resource;

    public TaggedResource(final String tag, final Resource resource) {
        this.tag = tag;
        this.resource = resource;
    }
}
