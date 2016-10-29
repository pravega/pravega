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

import com.emc.pravega.common.util.AvlTreeIndex;
import com.emc.pravega.common.util.IndexEntry;
import com.emc.pravega.common.util.RedBlackTreeIndex;
import com.emc.pravega.common.util.SortedIndex;

/**
 * Playground Test class.
 */
public class Playground {

    public static void main(String[] args) throws Exception {
        //        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        //        context.getLoggerList().get(0).setLevel(Level.INFO);
        //context.reset();
        compareTrees();
    }

    private static void compareTrees() {
        int count = 10000000;
        int retryCount = 5;
        for (int i = 0; i < retryCount; i++) {
            System.out.println("RB");
            System.gc();
            testIndex(new RedBlackTreeIndex<>(Integer::compare), count);
            System.out.println("AVL");
            System.gc();
            testIndex(new AvlTreeIndex<>(Integer::compare), count);
        }

        //        int testCount = 100;
        //        int skip = 1;
        //        for (int i = 0; i < testCount; i += skip) {
        //            tm.put(i, new TestEntry(i));
        //            testTree.insert(new TestEntry(i));
        //        }
        //        for (int i = 0; i < testCount; i++) {
        //            val en = tm.ceilingEntry(i);
        //            val av = testTree.getCeiling(i);
        //            if ((en != null && av == null)
        //                    || (en == null && av != null)
        //                    || (en != null && en.getKey() != av.key())) {
        //                System.out.println(String.format("Key = %s, E = %s, A = %s", i, en, av));
        //            }
        //        }

        //        for (int i = 0; i < testCount; i++) {
        //            if (i % 2 == 0) {
        //                testTree.remove(i);
        //            }
        //        }
        //
        //        for (int i = 0; i < testCount; i += skip) {
        //            if (i % 2 == 0) {
        //                testTree.remove(i);
        //                System.out.print(i + ":");
        //                testTree.forEach(e -> System.out.print(" " + e));
        //                System.out.println();
        //            }
        //        }
        //
        //        //        testTree.forEach(e -> System.out.print(" " + e));
        //        System.out.println();
    }

    private static void testIndex(SortedIndex<Integer, TestEntry> index, int count) {
        long testInsertElapsed = measure(() -> insert(index, 0, count));
        long testReadElapsed = measure(() -> readExact(index, 0, count));
        long testReadCeilingElapsed = measure(() -> readCeiling(index, 0, count));
        System.out.println(String.format("Insert = %sms, Read = %sms, Ceiling = %sms", testInsertElapsed, testReadElapsed, testReadCeilingElapsed));
    }

    private static void insert(SortedIndex<Integer, TestEntry> rbt, int start, int count) {
        int max = start + count;
        for (int i = start; i < max; i++) {
            rbt.put(new TestEntry(i));
        }
    }

    private static void readExact(SortedIndex<Integer, TestEntry> rbt, int start, int count) {
        int max = start + count;
        for (int i = start; i < max; i++) {
            rbt.get(i);
        }
    }

    private static void readCeiling(SortedIndex<Integer, TestEntry> rbt, int start, int count) {
        int max = start + count;
        for (int i = start; i < max; i++) {
            rbt.getCeiling(i);
        }
    }

    private static int measure(Runnable r) {
        long rbtStart = System.nanoTime();
        r.run();
        return (int) ((System.nanoTime() - rbtStart) / 1000 / 1000);
    }

    static class TestEntry implements IndexEntry<Integer> {
        final int key;

        TestEntry(int key) {
            this.key = key;
        }

        @Override
        public Integer key() {
            return this.key;
        }

        @Override
        public String toString() {
            return Integer.toString(this.key);
        }
    }
}
