/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.metadata;

import io.pravega.segmentstore.storage.mocks.InMemoryMetadataStore;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for ChunkMetadataStoreTests.
 */
public class ChunkMetadataStoreTests {

    public static final String KEY0 = "donald";
    public static final String KEY1 = "micky";
    public static final String KEY3 = "pluto";

    public static final String VALUE0 = "duck";
    public static final String VALUE1 = "rat";
    public static final String VALUE2 = "dog";
    public static final String VALUE4 = "mouse";
    public static final String VALUE5 = "avian";
    public static final String VALUE6 = "bird";

    @Rule
    public Timeout globalTimeout = Timeout.seconds(300);

    protected ChunkMetadataStore metadataStore;


    @Before
    public void setUp() {
        metadataStore = new InMemoryMetadataStore();
    }

    @After
    public void tearDown() {
    }


    @Test
    public void testSimpleScenario() throws Exception {
        try (MetadataTransaction txn = metadataStore.beginTransaction()) {
            // NO value should be found.
            Assert.assertNull(metadataStore.get(txn, KEY0));
            Assert.assertNull(metadataStore.get(txn, KEY1));
            Assert.assertNull(metadataStore.get(txn, KEY3));

            // Create data
            metadataStore.create(txn, new TestData(KEY0, VALUE0));
            metadataStore.create(txn, new TestData(KEY1, VALUE1));
            metadataStore.create(txn, new TestData(KEY3, VALUE2));

            assertEquals((TestData) metadataStore.get(txn, KEY0), KEY0, VALUE0);
            assertEquals((TestData) metadataStore.get(txn, KEY1), KEY1, VALUE1);
            assertEquals((TestData) metadataStore.get(txn, KEY3), KEY3, VALUE2);

            // Update
            metadataStore.create(txn, new TestData(KEY1, VALUE4));
            assertEquals((TestData) metadataStore.get(txn, KEY0), KEY0, VALUE0);
            assertEquals((TestData) metadataStore.get(txn, KEY1), KEY1, VALUE4);
            assertEquals((TestData) metadataStore.get(txn, KEY3), KEY3, VALUE2);

            // delete
            metadataStore.delete(txn, KEY3);
            assertEquals((TestData) metadataStore.get(txn, KEY0), KEY0, VALUE0);
            assertEquals((TestData) metadataStore.get(txn, KEY1), KEY1, VALUE4);
            Assert.assertNull(metadataStore.get(txn, KEY3));
            metadataStore.commit(txn, false);

            assertEquals((TestData) metadataStore.get(txn, KEY0), KEY0, VALUE0);
            assertEquals((TestData) metadataStore.get(txn, KEY1), KEY1, VALUE4);
            Assert.assertNull(metadataStore.get(txn, KEY3));

        } catch (Exception e) {
            throw e;
        }

        // See the effect of transaction after words
        try (MetadataTransaction txn2 = metadataStore.beginTransaction()) {
            assertEquals((TestData) metadataStore.get(txn2, KEY0), KEY0, VALUE0);
            assertEquals((TestData) metadataStore.get(txn2, KEY1), KEY1, VALUE4);
            Assert.assertNull(metadataStore.get(txn2, KEY3));

            metadataStore.update(txn2, new TestData(KEY0, VALUE6));
            metadataStore.delete(txn2, KEY1);
            assertEquals((TestData) metadataStore.get(txn2, KEY0), KEY0, VALUE6);
            Assert.assertNull(metadataStore.get(txn2, KEY3));
            Assert.assertNull(metadataStore.get(txn2, KEY1));
            // Implicitly aborted
        } catch (Exception e) {
            throw e;
        }

        // Should have no effect;
        try (MetadataTransaction txn3 = metadataStore.beginTransaction()) {
            assertEquals((TestData) metadataStore.get(txn3, KEY0), KEY0, VALUE0);
            assertEquals((TestData) metadataStore.get(txn3, KEY1), KEY1, VALUE4);
            Assert.assertNull(metadataStore.get(txn3, KEY3));
        } catch (Exception e) {
            throw e;
        }
    }


    @Test
    public void testSimpleScenarioWithLazyCommit() throws Exception {
        try (MetadataTransaction txn = metadataStore.beginTransaction()) {
            // NO value should be found.
            Assert.assertNull(metadataStore.get(txn, KEY0));
            Assert.assertNull(metadataStore.get(txn, KEY1));
            Assert.assertNull(metadataStore.get(txn, KEY3));

            // Create data
            metadataStore.create(txn, new TestData(KEY0, VALUE0));
            metadataStore.create(txn, new TestData(KEY1, VALUE1));
            metadataStore.create(txn, new TestData(KEY3, VALUE2));

            assertEquals((TestData) metadataStore.get(txn, KEY0), KEY0, VALUE0);
            assertEquals((TestData) metadataStore.get(txn, KEY1), KEY1, VALUE1);
            assertEquals((TestData) metadataStore.get(txn, KEY3), KEY3, VALUE2);

            // Update
            metadataStore.create(txn, new TestData(KEY1, VALUE4));
            assertEquals((TestData) metadataStore.get(txn, KEY0), KEY0, VALUE0);
            assertEquals((TestData) metadataStore.get(txn, KEY1), KEY1, VALUE4);
            assertEquals((TestData) metadataStore.get(txn, KEY3), KEY3, VALUE2);

            // delete
            metadataStore.delete(txn, KEY3);
            assertEquals((TestData) metadataStore.get(txn, KEY0), KEY0, VALUE0);
            assertEquals((TestData) metadataStore.get(txn, KEY1), KEY1, VALUE4);
            Assert.assertNull(metadataStore.get(txn, KEY3));
            metadataStore.commit(txn, true);

            assertEquals((TestData) metadataStore.get(txn, KEY0), KEY0, VALUE0);
            assertEquals((TestData) metadataStore.get(txn, KEY1), KEY1, VALUE4);
            Assert.assertNull(metadataStore.get(txn, KEY3));

        } catch (Exception e) {
            throw e;
        }

        // See the effect of transaction after words
        try (MetadataTransaction txn2 = metadataStore.beginTransaction()) {
            assertEquals((TestData) metadataStore.get(txn2, KEY0), KEY0, VALUE0);
            assertEquals((TestData) metadataStore.get(txn2, KEY1), KEY1, VALUE4);
            Assert.assertNull(metadataStore.get(txn2, KEY3));

            metadataStore.update(txn2, new TestData(KEY0, VALUE6));
            metadataStore.delete(txn2, KEY1);
            assertEquals((TestData) metadataStore.get(txn2, KEY0), KEY0, VALUE6);
            Assert.assertNull(metadataStore.get(txn2, KEY3));
            Assert.assertNull(metadataStore.get(txn2, KEY1));
            // Implicitly aborted
        } catch (Exception e) {
            throw e;
        }

        // Should have no effect;
        try (MetadataTransaction txn3 = metadataStore.beginTransaction()) {
            assertEquals((TestData) metadataStore.get(txn3, KEY0), KEY0, VALUE0);
            assertEquals((TestData) metadataStore.get(txn3, KEY1), KEY1, VALUE4);
            Assert.assertNull(metadataStore.get(txn3, KEY3));
        } catch (Exception e) {
            throw e;
        }
    }


    @Ignore("Not sure this is a valid case. But keeping it for now.")
    @Test
    public void testTransactionFailedForMultipleDeletes() throws Exception {
        // Set up.
        try (MetadataTransaction txn = metadataStore.beginTransaction()) {
            Assert.assertNull(metadataStore.get(txn, KEY1));
            // Create data.
            metadataStore.create(txn, new TestData(KEY1, VALUE1));
            // Validate the data is present in local transaction view.
            assertEquals((TestData) metadataStore.get(txn, KEY1), KEY1, VALUE1);
            // Commit.
            metadataStore.commit(txn, false);
            // Validate same data is present after commit.
            assertEquals((TestData) metadataStore.get(txn, KEY1), KEY1, VALUE1);
        } catch (Exception e) {
            throw e;
        }

        // Step 1 : Start a parallel transaction.
        try (MetadataTransaction txn0 = metadataStore.beginTransaction()) {
            // Step 1 A : In new transaction, delete a key.
            metadataStore.delete(txn0, KEY1);
            // Step 1 B : Validate data is not present in local transaction view before commit.
            Assert.assertNull(metadataStore.get(txn0, KEY1));

            // Step 2 : Start yet another parallel transaction.
            try (MetadataTransaction txn2 = metadataStore.beginTransaction()) {
                // Data should be visible in a second transaction.
                assertEquals((TestData) metadataStore.get(txn2, KEY1), KEY1, VALUE1);
                // Step 2 A: delete a key.
                metadataStore.delete(txn2, KEY1);
                Assert.assertNull(metadataStore.get(txn2, KEY1));

                // Step 2 B: commit
                metadataStore.commit(txn2, false);
                Assert.assertNull(metadataStore.get(txn2, KEY1));
            } catch (Exception e) {
                throw e;
            }

            // Step 3 : Now commit, which should fail.
            try {
                metadataStore.commit(txn0, false);
                Assert.fail("Transaction should be aborted.");
            } catch (StorageMetadataVersionMismatchException e) {
            }
        }
    }

    @Test
    public void testTransactionFailedForMultipleUpdates() throws Exception {
        // Step 1: Set up
        try (MetadataTransaction txn = metadataStore.beginTransaction()) {
            Assert.assertNull(metadataStore.get(txn, KEY0));
            Assert.assertNull(metadataStore.get(txn, KEY1));
            Assert.assertNull(metadataStore.get(txn, KEY3));
            // Create data
            metadataStore.create(txn, new TestData(KEY0, VALUE0));
            metadataStore.create(txn, new TestData(KEY1, VALUE1));
            metadataStore.create(txn, new TestData(KEY3, VALUE2));

            assertEquals((TestData) metadataStore.get(txn, KEY0), KEY0, VALUE0);
            assertEquals((TestData) metadataStore.get(txn, KEY1), KEY1, VALUE1);
            assertEquals((TestData) metadataStore.get(txn, KEY3), KEY3, VALUE2);

            metadataStore.commit(txn, false);

            // Same data after commit.
            assertEquals((TestData) metadataStore.get(txn, KEY0), KEY0, VALUE0);
            assertEquals((TestData) metadataStore.get(txn, KEY1), KEY1, VALUE1);
            assertEquals((TestData) metadataStore.get(txn, KEY3), KEY3, VALUE2);

        } catch (Exception e) {
            throw e;
        }

        // Step 2: Create a test transaction
        try (MetadataTransaction txn0 = metadataStore.beginTransaction()) {

            // Step 2 A: Make some updates, but don't commit yet.
            metadataStore.update(txn0, new TestData(KEY0, VALUE5));
            assertEquals((TestData) metadataStore.get(txn0, KEY0), KEY0, VALUE5);
            assertEquals((TestData) metadataStore.get(txn0, KEY3), KEY3, VALUE2);

            // Step 3 : Start a parallel transaction
            try (MetadataTransaction txn2 = metadataStore.beginTransaction()) {
                // Step 3 A: Make some updates, but don't commit yet.
                metadataStore.update(txn2, new TestData(KEY0, VALUE6));
                assertEquals((TestData) metadataStore.get(txn2, KEY0), KEY0, VALUE6);
                assertEquals((TestData) metadataStore.get(txn2, KEY3), KEY3, VALUE2);

                // Step 3 B: Commit. It should succeed.
                metadataStore.commit(txn2, false);
                assertEquals((TestData) metadataStore.get(txn2, KEY0), KEY0, VALUE6);
                assertEquals((TestData) metadataStore.get(txn2, KEY3), KEY3, VALUE2);

            } catch (Exception e) {
                throw e;
            }

            // Step 4 : Start a second parallel transaction
            try (MetadataTransaction txn2 = metadataStore.beginTransaction()) {
                // Step 4 : Data committed by earlier transaction should be visible to other new transactions
                assertEquals((TestData) metadataStore.get(txn2, KEY0), KEY0, VALUE6);
                assertEquals((TestData) metadataStore.get(txn2, KEY3), KEY3, VALUE2);
            } catch (Exception e) {
                throw e;
            }

            // A committed transactions should not have any effect on this transaction.
            assertEquals((TestData) metadataStore.get(txn0, KEY0), KEY0, VALUE5);
            assertEquals((TestData) metadataStore.get(txn0, KEY3), KEY3, VALUE2);

            try {
                metadataStore.commit(txn0, false);
                Assert.fail("Transaction should be aborted.");
            } catch (StorageMetadataVersionMismatchException e) {
            }
        }
    }

    @Test
    public void testTransactionFailedForMultipleOperations() throws Exception {

        // Set up the data there are 3 keys
        try (MetadataTransaction txn = metadataStore.beginTransaction()) {
            Assert.assertNull(metadataStore.get(txn, KEY0));
            Assert.assertNull(metadataStore.get(txn, KEY1));
            Assert.assertNull(metadataStore.get(txn, KEY3));
            // Create data
            metadataStore.create(txn, new TestData(KEY0, VALUE0));
            metadataStore.create(txn, new TestData(KEY1, VALUE1));
            metadataStore.create(txn, new TestData(KEY3, VALUE2));

            assertEquals((TestData) metadataStore.get(txn, KEY0), KEY0, VALUE0);
            assertEquals((TestData) metadataStore.get(txn, KEY1), KEY1, VALUE1);
            assertEquals((TestData) metadataStore.get(txn, KEY3), KEY3, VALUE2);

            metadataStore.commit(txn, false);

            // Same data after commit.
            assertEquals((TestData) metadataStore.get(txn, KEY0), KEY0, VALUE0);
            assertEquals((TestData) metadataStore.get(txn, KEY1), KEY1, VALUE1);
            assertEquals((TestData) metadataStore.get(txn, KEY3), KEY3, VALUE2);

        } catch (Exception e) {
            throw e;
        }

        // Start a test transaction
        try (MetadataTransaction txn0 = metadataStore.beginTransaction()) {
            // Update/delete some keys but don't commit .
            // We wan to see that these changes do not affect other transactions
            metadataStore.update(txn0, new TestData(KEY0, VALUE5));
            metadataStore.delete(txn0, KEY1);
            assertEquals((TestData) metadataStore.get(txn0, KEY0), KEY0, VALUE5);
            assertEquals((TestData) metadataStore.get(txn0, KEY3), KEY3, VALUE2);
            Assert.assertNull(metadataStore.get(txn0, KEY1));

            // Another Transaction that must be able modify its view data and commit
            try (MetadataTransaction txn2 = metadataStore.beginTransaction()) {

                metadataStore.update(txn2, new TestData(KEY0, VALUE6));
                metadataStore.delete(txn2, KEY1);
                assertEquals((TestData) metadataStore.get(txn2, KEY0), KEY0, VALUE6);
                assertEquals((TestData) metadataStore.get(txn2, KEY3), KEY3, VALUE2);
                Assert.assertNull(metadataStore.get(txn2, KEY1));
                metadataStore.commit(txn2, false);
                assertEquals((TestData) metadataStore.get(txn2, KEY0), KEY0, VALUE6);
                assertEquals((TestData) metadataStore.get(txn2, KEY3), KEY3, VALUE2);
                Assert.assertNull(metadataStore.get(txn2, KEY1));
            } catch (Exception e) {
                throw e;
            }

            // Yet another Transaction that must be able modify its view data and commit
            try (MetadataTransaction txn2 = metadataStore.beginTransaction()) {

                assertEquals((TestData) metadataStore.get(txn2, KEY0), KEY0, VALUE6);
                assertEquals((TestData) metadataStore.get(txn2, KEY3), KEY3, VALUE2);
                Assert.assertNull(metadataStore.get(txn2, KEY1));
            } catch (Exception e) {
                throw e;
            }

            // A committed transaction should not have any effect on this transaction.
            assertEquals((TestData) metadataStore.get(txn0, KEY0), KEY0, VALUE5);
            assertEquals((TestData) metadataStore.get(txn0, KEY3), KEY3, VALUE2);
            Assert.assertNull(metadataStore.get(txn0, KEY1));

            // However when it tries to commit this should fail.
            try {
                metadataStore.commit(txn0, false);
                Assert.fail("Transaction should be aborted.");
            } catch (StorageMetadataVersionMismatchException e) {

            }
        }
    }

    private void assertEquals(TestData data, String key, String value) {
        Assert.assertEquals("Should get the same data",  key, data.getKey());
        Assert.assertEquals("Should get the same data", value, data.getValue());
    }

    /**
     *
     */
    @Builder(toBuilder = true)
    static class TestData implements StorageMetadata {

        @Getter
        @Setter
        String key;

        @Getter
        @Setter
        String value;

        /**
         *
         * @param key
         * @param value
         */
        public TestData(String key, String value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public StorageMetadata copy() {
            return toBuilder().build();
        }
    }
}
