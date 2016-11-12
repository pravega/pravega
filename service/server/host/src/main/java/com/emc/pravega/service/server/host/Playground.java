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

package com.emc.pravega.service.server.host;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.emc.pravega.common.util.PropertyBag;
import com.emc.pravega.service.server.CacheKey;
import com.emc.pravega.service.storage.Cache;
import com.emc.pravega.service.storage.impl.rocksdb.RocksDBCacheFactory;
import com.emc.pravega.service.storage.impl.rocksdb.RocksDBConfig;
import lombok.Cleanup;
import org.slf4j.LoggerFactory;

/**
 * Playground Test class.
 */
public class Playground {

    public static void main(String[] args) throws Exception {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.getLoggerList().get(0).setLevel(Level.INFO);
        //context.reset();

        RocksDBConfig config = new RocksDBConfig(PropertyBag.create());

        @Cleanup
        RocksDBCacheFactory factory = new RocksDBCacheFactory(config);
        factory.initialize(true);
        final String cacheId = "MockCache";
        Cache cache = factory.getCache(cacheId);

        long maxSegmentId = 100;
        long maxOffsetId = 100;

        //byte[] data = String.format("SegmentId=%s,Offset=%s", maxSegmentId, maxOffsetId).getBytes();
        long writeStart = System.nanoTime();
        for (long segmentId = 0; segmentId < maxSegmentId; segmentId++) {
            for (long offset = 0; offset < maxOffsetId; offset++) {
                Cache.Key key = new CacheKey(segmentId, offset);
                byte[] data = String.format("SegmentId=%s,Offset=%s", segmentId, offset).getBytes();
                cache.insert(key, data);
                System.out.println(String.format("SegmentId=%s,Offset=%s", segmentId, offset));
            }
        }

        long writeElapsed = System.nanoTime() - writeStart;

        long readStart = System.nanoTime();
        for (long segmentId = 0; segmentId < maxSegmentId; segmentId++) {
            for (long offset = 0; offset < maxOffsetId; offset++) {
                Cache.Key key = new CacheKey(segmentId, offset);
                byte[] readData = cache.get(key);
                String dataString = readData == null ? "(null)" : new String(readData);
                System.out.println(String.format("SegmentId=%s,Offset=%d - %s", segmentId, offset, dataString));
            }
        }

        long readElapsed = System.nanoTime() - readStart;
        System.out.println(String.format("Count = %s, Write = %sms, Read = %sms", maxSegmentId * maxOffsetId,
                writeElapsed / 1000 / 1000, readElapsed / 1000 / 1000));
    }
}
