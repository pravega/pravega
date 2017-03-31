/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */

package com.emc.pravega.common.util;

import com.google.common.base.Preconditions;

/**
 * Validates names. The convention we are following is that
 * names must contain only alphanumeric characters.
 */
public class NameVerifier {

    /**
     * Validates a name.
     *
     * @param name Stream name to validate.
     * @return The stream name in the case is valid.
     */
    public static String validateName(String name) {
        Preconditions.checkNotNull(name);
        Preconditions.checkArgument(name.matches("^\\w+\\z"), "Name must be [a-zA-Z0-9]*");

        return name;
    }
}
