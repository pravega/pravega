/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables;

import io.pravega.common.util.HashedArray;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;

/**
 * Helper methods for testing with {@link TableEntry} instances.
 */
final class TableEntryHelpers {

    static boolean areEqual(TableEntry e1, TableEntry e2) {
        return areEqual(e1.getKey(), e2.getKey())
                && HashedArray.arrayEquals(e1.getValue(), e2.getValue());
    }

    static boolean areEqual(TableKey k1, TableKey k2) {
        return HashedArray.arrayEquals(k1.getKey(), k2.getKey())
                && k1.getVersion() == k2.getVersion();
    }
}
