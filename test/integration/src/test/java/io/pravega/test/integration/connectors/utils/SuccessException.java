/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.test.integration.connectors.utils;

/**
 * Special marker exception that aborts a program but indicates success.
 * Workaround for finite tests on infinite inputs.
 */
public class SuccessException extends Exception {
}
