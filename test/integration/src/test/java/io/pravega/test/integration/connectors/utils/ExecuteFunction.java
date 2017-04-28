/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.test.integration.connectors.utils;

/**
 * Simple function interface for {@code () -> void }.
 */
@FunctionalInterface
public interface ExecuteFunction {

    void execute();
}
