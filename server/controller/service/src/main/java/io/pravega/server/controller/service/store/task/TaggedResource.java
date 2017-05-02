/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.server.controller.service.store.task;

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
