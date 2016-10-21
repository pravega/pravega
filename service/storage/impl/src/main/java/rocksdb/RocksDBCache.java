/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package rocksdb;

import com.emc.pravega.common.util.ByteArraySegment;
import com.emc.pravega.service.storage.Cache;

/**
 * RocksDB-backed Cache.
 */
public class RocksDBCache implements Cache {

    //region AutoCloseable Implementation

    @Override
    public void close() {

    }

    //endregion

    //region Cache Implementation

    @Override
    public String getId() {
        return null;
    }

    @Override
    public void insert(Key key, byte[] data) {

    }

    @Override
    public void insert(Key key, ByteArraySegment data) {

    }

    @Override
    public byte[] get(Key key) {
        return new byte[0];
    }

    @Override
    public boolean remove(Key key) {
        return false;
    }

    @Override
    public void reset() {

    }

    //endregion
}
