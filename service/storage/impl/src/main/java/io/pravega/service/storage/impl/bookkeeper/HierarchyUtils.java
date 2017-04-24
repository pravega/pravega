/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.bookkeeper;

/**
 * Utility methods for building a hierarchical node structure.
 */
final class HierarchyUtils {
    /**
     * Node path separator.
     */
    private static final String SEPARATOR = "/";

    /**
     * Used for extracting intermediate values for the path.
     */
    private static final int DIVISOR = 10;

    /**
     * Gets a hierarchical path rooted at the given namespace of the given depth, using the value of nodeId to construct
     * the intermediate paths.
     * Examples:
     * * root = "/root", nodeId = 1234, depth = 3 -> /root/4/3/2/1234
     * * root = "/root", nodeId = 1234, depth = 0 -> /root/1234
     * * root = "/root", nodeId = 1234, depth = 6 -> /root/4/3/2/1/0/0/1234
     *
     * @param root   The root namespace.
     * @param nodeId The node id to create the path for.
     * @param depth  The hierarchy depth (0 means flat (root + value)).
     * @return The hierarchical path.
     */
    static String getPath(String root, int nodeId, int depth) {
        if (root.endsWith(SEPARATOR)) {
            root = root.substring(0, root.length() - SEPARATOR.length());
        }

        StringBuilder pathBuilder = new StringBuilder(root);
        int value = nodeId;
        for (int i = 0; i < depth; i++) {
            int r = value % DIVISOR;
            value = value / DIVISOR;
            pathBuilder.append(SEPARATOR).append(r);
        }

        return pathBuilder.append(SEPARATOR).append(nodeId).toString();
    }
}
