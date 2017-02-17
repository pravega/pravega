/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
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
