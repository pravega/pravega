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
import io.pravega.segmentstore.storage.mocks.MockStorageMetadata;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Unit tests for ChunkMetadataStoreTests.
 */
public class ChunkMetadataStoreTests extends ThreadPooledTestSuite {

    protected static final String KEY0 = "donald";
    protected static final String KEY2 = "goofy";
    protected static final String KEY1 = "micky";
    protected static final String KEY3 = "pluto";
    protected static final String KEY4 = "mini";

    protected static final String VALUE0 = "duck";
    protected static final String VALUE1 = "rat";
    protected static final String VALUE2 = "dog";
    protected static final String VALUE4 = "mouse";
    protected static final String VALUE5 = "avian";
    protected static final String VALUE6 = "bird";

    private static final String[] KEYS = new String[]{ KEY0, KEY1, KEY2, KEY3};

    @Rule
    public Timeout globalTimeout = Timeout.seconds(300);

    protected BaseMetadataStore metadataStore;

    @Before
    public void setUp() throws Exception {
        super.before();
        metadataStore = new InMemoryMetadataStore(executorService());
    }

    @After
    public void tearDown() throws Exception {
        super.after();
    }

    /**
     * Test basic invariants
     *
     * @throws Exception Exception if any.
     */
    @Test
    public void testInvariants() throws Exception {
        try (MetadataTransaction txn = metadataStore.beginTransaction(false, KEYS)) {
            Assert.assertFalse(txn.isAborted());
            Assert.assertFalse(txn.isCommitted());
            assertNull(txn.get(null));

            txn.abort();
            Assert.assertTrue(txn.isAborted());

            AssertExtensions.assertThrows(
                    "abort should throw an excpetion",
                    () -> txn.abort(),
                    ex -> ex instanceof IllegalStateException);

            AssertExtensions.assertThrows(
                    "openWrite succeeded when exception was expected.",
                    () -> txn.commit(),
                    ex -> ex instanceof IllegalStateException);
        }
    }

    /**
     * Test delete
     *
     * @throws Exception Exception if any.
     */
    @Test
    public void testDelete() throws Exception {
        try (MetadataTransaction txn = metadataStore.beginTransaction(false, KEYS)) {
            assertNull(txn.get(KEY0));
            txn.create(new MockStorageMetadata(KEY0, VALUE0));
            Assert.assertNotNull(txn.get(KEY0));
            txn.delete(KEY0);
            assertNull(txn.get(KEY0));
            txn.commit().join();
        }

        try (MetadataTransaction txn = metadataStore.beginTransaction(false, KEYS)) {
            assertNull(txn.get(KEY0));
            // Should be able to recreate
            txn.create(new MockStorageMetadata(KEY0, VALUE0));
            assertEquals(txn.get(KEY0), KEY0, VALUE0);
            txn.commit().join();
        }

        try (MetadataTransaction txn = metadataStore.beginTransaction(false, KEYS)) {
            assertEquals(txn.get(KEY0), KEY0, VALUE0);
        }
    }

    /**
     * Test a simple scenario.
     *
     * @throws Exception Exception if any.
     */
    @Test
    public void testSimpleScenario() throws Exception {
        try (MetadataTransaction txn = metadataStore.beginTransaction(false, KEYS)) {
            // NO value should be found.
            assertNull(txn.get(KEY0));
            assertNull(txn.get(KEY1));
            assertNull(txn.get(KEY3));

            // Create data
            txn.create(new MockStorageMetadata(KEY0, VALUE0));
            txn.create(new MockStorageMetadata(KEY1, VALUE1));
            txn.create(new MockStorageMetadata(KEY2, VALUE2));
            // Do not access data before
            txn.create(new MockStorageMetadata(KEY4, VALUE4));

            assertEquals(txn.get(KEY0), KEY0, VALUE0);
            assertEquals(txn.get(KEY1), KEY1, VALUE1);
            assertEquals(txn.get(KEY2), KEY2, VALUE2);
            assertEquals(txn.get(KEY4), KEY4, VALUE4);

            // Update
            txn.update(new MockStorageMetadata(KEY1, VALUE4));
            assertEquals(txn.get(KEY0), KEY0, VALUE0);
            assertEquals(txn.get(KEY1), KEY1, VALUE4);
            assertEquals(txn.get(KEY2), KEY2, VALUE2);

            // create
            txn.create(new MockStorageMetadata(KEY3, VALUE5));
            assertEquals(txn.get(KEY0), KEY0, VALUE0);
            assertEquals(txn.get(KEY1), KEY1, VALUE4);
            assertEquals(txn.get(KEY2), KEY2, VALUE2);
            assertEquals(txn.get(KEY3), KEY3, VALUE5);

            // delete
            txn.delete(KEY2);
            txn.delete(KEY3);
            assertEquals(txn.get(KEY0), KEY0, VALUE0);
            assertEquals(txn.get(KEY1), KEY1, VALUE4);
            assertNull(txn.get(KEY3));
            txn.commit().join();

            assertEquals(txn.get(KEY0), KEY0, VALUE0);
            assertEquals(txn.get(KEY1), KEY1, VALUE4);
            assertEquals(txn.get(KEY4), KEY4, VALUE4);
            assertNull(txn.get(KEY2));
            assertNull(txn.get(KEY3));
        } catch (Exception e) {
            throw e;
        }

        // make sure they are stored correctly
        assertEquals((MockStorageMetadata) metadataStore.read(KEY0).get().getValue(), KEY0, VALUE0);
        assertEquals((MockStorageMetadata) metadataStore.read(KEY1).get().getValue(), KEY1, VALUE4);
        Assert.assertNull(metadataStore.read(KEY2).get().getValue());
        Assert.assertNull(metadataStore.read(KEY3).get().getValue());

        // See the effect of transaction after words
        try (MetadataTransaction txn2 = metadataStore.beginTransaction(false, KEYS)) {
            assertEquals(txn2.get(KEY0), KEY0, VALUE0);
            assertEquals(txn2.get(KEY1), KEY1, VALUE4);
            assertNull(txn2.get(KEY3));

            txn2.update(new MockStorageMetadata(KEY0, VALUE6));
            txn2.delete(KEY1);
            assertEquals(txn2.get(KEY0), KEY0, VALUE6);
            assertNull(txn2.get(KEY3));
            assertNull(txn2.get(KEY1));
            // Implicitly aborted
        } catch (Exception e) {
            throw e;
        }

        // Should have no effect;
        try (MetadataTransaction txn3 = metadataStore.beginTransaction(false, KEYS)) {
            assertEquals(txn3.get(KEY0), KEY0, VALUE0);
            assertEquals(txn3.get(KEY1), KEY1, VALUE4);
            assertNull(txn3.get(KEY2));
            assertNull(txn3.get(KEY3));
        } catch (Exception e) {
            throw e;
        }

        // make sure they are stored correctly
        assertEquals((MockStorageMetadata) metadataStore.read(KEY0).get().getValue(), KEY0, VALUE0);
        assertEquals((MockStorageMetadata) metadataStore.read(KEY1).get().getValue(), KEY1, VALUE4);
        Assert.assertNull(metadataStore.read(KEY2).get().getValue());
        Assert.assertNull(metadataStore.read(KEY3).get().getValue());
    }

    /**
     * Test a simple scenario with no buffering.
     *
     * @throws Exception Exception if any.
     */
    @Test
    public void testScenariosWithNoBuffering() throws Exception {
        metadataStore.setMaxEntriesInTxnBuffer(0);
        testSimpleScenario();
    }

    /**
     * Tests simple scenario with lazy commit.
     *
     * @throws Exception
     */
    @Test
    public void testSimpleScenarioWithLazyCommit() throws Exception {
        try (MetadataTransaction txn = metadataStore.beginTransaction(false, KEYS)) {
            // NO value should be found.
            assertNull(txn.get(KEY0));
            assertNull(txn.get(KEY1));
            assertNull(txn.get(KEY2));

            // Create KEY0, KEY1, KEY2
            txn.create(new MockStorageMetadata(KEY0, VALUE0));
            txn.create(new MockStorageMetadata(KEY1, VALUE1));
            txn.create(new MockStorageMetadata(KEY2, VALUE2));

            assertEquals(txn.get(KEY0), KEY0, VALUE0);
            assertEquals(txn.get(KEY1), KEY1, VALUE1);
            assertEquals(txn.get(KEY2), KEY2, VALUE2);

            // Update KEY1
            txn.update(new MockStorageMetadata(KEY1, VALUE4));
            assertEquals(txn.get(KEY0), KEY0, VALUE0);
            assertEquals(txn.get(KEY1), KEY1, VALUE4);
            assertEquals(txn.get(KEY2), KEY2, VALUE2);

            // create KEY3
            txn.create(new MockStorageMetadata(KEY3, VALUE5));
            assertEquals(txn.get(KEY0), KEY0, VALUE0);
            assertEquals(txn.get(KEY1), KEY1, VALUE4);
            assertEquals(txn.get(KEY2), KEY2, VALUE2);
            assertEquals(txn.get(KEY3), KEY3, VALUE5);

            // delete KEY2, KEY3
            txn.delete(KEY2);
            txn.delete(KEY3);
            assertEquals(txn.get(KEY0), KEY0, VALUE0);
            assertEquals(txn.get(KEY1), KEY1, VALUE4);
            assertNull(txn.get(KEY2));
            assertNull(txn.get(KEY3));
            txn.commit(true).join();

            assertEquals(txn.get(KEY0), KEY0, VALUE0);
            assertEquals(txn.get(KEY1), KEY1, VALUE4);
            assertNull(txn.get(KEY2));
            assertNull(txn.get(KEY3));

            // make sure they are stored correctly
            Assert.assertNull(metadataStore.read(KEY0).get().getValue());
            Assert.assertNull(metadataStore.read(KEY1).get().getValue());
            Assert.assertNull(metadataStore.read(KEY2).get().getValue());
            Assert.assertNull(metadataStore.read(KEY3).get().getValue());

        } catch (Exception e) {
            throw e;
        }

        // See the effect of transaction after words
        try (MetadataTransaction txn2 = metadataStore.beginTransaction(false, KEYS)) {
            assertEquals(txn2.get(KEY0), KEY0, VALUE0);
            assertEquals(txn2.get(KEY1), KEY1, VALUE4);
            assertNull(txn2.get(KEY3));

            txn2.update(new MockStorageMetadata(KEY0, VALUE6));
            txn2.delete(KEY1);
            assertEquals(txn2.get(KEY0), KEY0, VALUE6);
            assertNull(txn2.get(KEY3));
            assertNull(txn2.get(KEY1));
            // Implicitly aborted
        } catch (Exception e) {
            throw e;
        }

        // Should have no effect;
        try (MetadataTransaction txn3 = metadataStore.beginTransaction(false, KEYS)) {
            assertEquals(txn3.get(KEY0), KEY0, VALUE0);
            assertEquals(txn3.get(KEY1), KEY1, VALUE4);
            assertNull(txn3.get(KEY3));
        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * Tests simple scenario with lazy commit.
     *
     * @throws Exception
     */
    @Test
    public void testSimpleScenarioWithPinnedRecords() throws Exception {
        try (MetadataTransaction txn = metadataStore.beginTransaction(false, KEYS)) {
            // NO value should be found.
            assertNull(txn.get(KEY0));
            assertNull(txn.get(KEY1));
            assertNull(txn.get(KEY2));

            // Create KEY0, KEY1, KEY2
            txn.create(new MockStorageMetadata(KEY0, VALUE0));
            txn.create(new MockStorageMetadata(KEY1, VALUE1));
            txn.create(new MockStorageMetadata(KEY2, VALUE2));
            assertEquals(txn.get(KEY0), KEY0, VALUE0);
            assertEquals(txn.get(KEY1), KEY1, VALUE1);
            assertEquals(txn.get(KEY2), KEY2, VALUE2);

            txn.markPinned(txn.get(KEY0).get());
            txn.markPinned(txn.get(KEY1).get());
            txn.markPinned(txn.get(KEY2).get());

            // Update KEY1
            txn.update(new MockStorageMetadata(KEY1, VALUE4));
            assertEquals(txn.get(KEY0), KEY0, VALUE0);
            assertEquals(txn.get(KEY1), KEY1, VALUE4);
            assertEquals(txn.get(KEY2), KEY2, VALUE2);

            // create KEY3
            txn.create(new MockStorageMetadata(KEY3, VALUE5));
            assertEquals(txn.get(KEY0), KEY0, VALUE0);
            assertEquals(txn.get(KEY1), KEY1, VALUE4);
            assertEquals(txn.get(KEY2), KEY2, VALUE2);
            assertEquals(txn.get(KEY3), KEY3, VALUE5);

            // delete KEY2, KEY3
            txn.delete(KEY2);
            txn.delete(KEY3);
            assertEquals(txn.get(KEY0), KEY0, VALUE0);
            assertEquals(txn.get(KEY1), KEY1, VALUE4);
            assertNull(txn.get(KEY2));
            assertNull(txn.get(KEY3));
            txn.commit().join();

            assertEquals(txn.get(KEY0), KEY0, VALUE0);
            assertEquals(txn.get(KEY1), KEY1, VALUE4);
            assertNull(txn.get(KEY2));
            assertNull(txn.get(KEY3));

            // make sure they are stored correctly
            Assert.assertNull(metadataStore.read(KEY0).get().getValue());
            Assert.assertNull(metadataStore.read(KEY1).get().getValue());
            Assert.assertNull(metadataStore.read(KEY2).get().getValue());
            Assert.assertNull(metadataStore.read(KEY3).get().getValue());

        } catch (Exception e) {
            throw e;
        }

        // See the effect of transaction after words
        try (MetadataTransaction txn2 = metadataStore.beginTransaction(false, KEYS)) {
            assertEquals(txn2.get(KEY0), KEY0, VALUE0);
            assertEquals(txn2.get(KEY1), KEY1, VALUE4);
            assertNull(txn2.get(KEY3));

            txn2.update(new MockStorageMetadata(KEY0, VALUE6));
            txn2.delete(KEY1);
            assertEquals(txn2.get(KEY0), KEY0, VALUE6);
            assertNull(txn2.get(KEY3));
            assertNull(txn2.get(KEY1));
            // Implicitly aborted
        } catch (Exception e) {
            throw e;
        }

        // Should have no effect;
        try (MetadataTransaction txn3 = metadataStore.beginTransaction(false, KEYS)) {
            assertEquals(txn3.get(KEY0), KEY0, VALUE0);
            assertEquals(txn3.get(KEY1), KEY1, VALUE4);
            assertNull(txn3.get(KEY3));
        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * Tests simple scenario with lazy commit.
     *
     * @throws Exception
     */
    @Test
    public void testSimpleScenarioWithPinnedRecordsWithNoBuffering() throws Exception {
        metadataStore.setMaxEntriesInTxnBuffer(0);
        testSimpleScenarioWithPinnedRecords();
    }

    /**
     * Tests simple scenario with lazy commit.
     *
     * @throws Exception
     */
    @Test
    public void testLazyCommitWithNormalCommitWithNoBuffer() throws Exception {
        metadataStore.setMaxEntriesInTxnBuffer(0);
        try (MetadataTransaction txn = metadataStore.beginTransaction(false, KEYS)) {
            // NO value should be found.
            assertNull(txn.get(KEY0));
            assertNull(txn.get(KEY1));
            assertNull(txn.get(KEY2));

            // Create data
            txn.create(new MockStorageMetadata(KEY0, VALUE0));
            txn.create(new MockStorageMetadata(KEY1, VALUE1));
            txn.create(new MockStorageMetadata(KEY2, VALUE2));

            assertEquals(txn.get(KEY0), KEY0, VALUE0);
            assertEquals(txn.get(KEY1), KEY1, VALUE1);
            assertEquals(txn.get(KEY2), KEY2, VALUE2);

            txn.commit().join(); // This is normal commit.

        } catch (Exception e) {
            throw e;
        }

        // make sure they are stored correctly
        assertEquals((MockStorageMetadata) metadataStore.read(KEY0).get().getValue(), KEY0, VALUE0);
        assertEquals((MockStorageMetadata) metadataStore.read(KEY1).get().getValue(), KEY1, VALUE1);
        assertEquals((MockStorageMetadata) metadataStore.read(KEY2).get().getValue(), KEY2, VALUE2);

        try (MetadataTransaction txn1 = metadataStore.beginTransaction(false, KEYS)) {
            // Update
            txn1.update(new MockStorageMetadata(KEY1, VALUE4));
            assertEquals(txn1.get(KEY0), KEY0, VALUE0);
            assertEquals(txn1.get(KEY1), KEY1, VALUE4);
            assertEquals(txn1.get(KEY2), KEY2, VALUE2);

            // delete
            txn1.delete(KEY2);
            assertEquals(txn1.get(KEY0), KEY0, VALUE0);
            assertEquals(txn1.get(KEY1), KEY1, VALUE4);
            assertNull(txn1.get(KEY2));
            txn1.commit(true).join(); // Commit lazily.

            assertEquals(txn1.get(KEY0), KEY0, VALUE0);
            assertEquals(txn1.get(KEY1), KEY1, VALUE4);
            assertNull(txn1.get(KEY2));

        } catch (Exception e) {
            throw e;
        }

        // make sure the persistent store actually has the correct value.
        assertEquals((MockStorageMetadata) metadataStore.read(KEY0).get().getValue(), KEY0, VALUE0);
        assertEquals((MockStorageMetadata) metadataStore.read(KEY1).get().getValue(), KEY1, VALUE4);
        Assert.assertNull(metadataStore.read(KEY2).get().getValue());

        // See the effect of transaction after words in a new transaction.
        // Note this transaction is aborted. So any changes made here should be totally ignored.
        try (MetadataTransaction txn2 = metadataStore.beginTransaction(false, KEYS)) {
            assertEquals(txn2.get(KEY0), KEY0, VALUE0);
            assertEquals(txn2.get(KEY1), KEY1, VALUE4);
            assertNull(txn2.get(KEY2));

            txn2.update(new MockStorageMetadata(KEY0, VALUE6));
            txn2.delete(KEY1);
            // This is still
            assertEquals(txn2.get(KEY0), KEY0, VALUE6);
            assertNull(txn2.get(KEY3));
            assertNull(txn2.get(KEY1));
            // abort
            txn2.abort();
        } catch (Exception e) {
            throw e;
        }
        // Should have no effect;
        try (MetadataTransaction txn3 = metadataStore.beginTransaction(false, KEYS)) {
            assertEquals(txn3.get(KEY0), KEY0, VALUE0);
            assertEquals(txn3.get(KEY1), KEY1, VALUE4);
            assertNull(txn3.get(KEY2));
        } catch (Exception e) {
            throw e;
        }

        try (MetadataTransaction txn4 = metadataStore.beginTransaction(false, KEYS)) {
            // Update
            txn4.update(new MockStorageMetadata(KEY0, VALUE5));
            txn4.update(new MockStorageMetadata(KEY1, VALUE6));
            assertEquals(txn4.get(KEY0), KEY0, VALUE5);
            assertEquals(txn4.get(KEY1), KEY1, VALUE6);
            assertNull(txn4.get(KEY2));

            // delete
            txn4.delete(KEY1);
            assertEquals(txn4.get(KEY0), KEY0, VALUE5);
            assertNull(txn4.get(KEY1));
            assertNull(txn4.get(KEY2));
            txn4.commit().join();

            assertEquals(txn4.get(KEY0), KEY0, VALUE5);
            assertNull(txn4.get(KEY1));
            assertNull(txn4.get(KEY2));

        } catch (Exception e) {
            throw e;
        }

        // make sure the persistent store is not affected.
        assertEquals((MockStorageMetadata) metadataStore.read(KEY0).get().getValue(), KEY0, VALUE5);
        Assert.assertNull(metadataStore.read(KEY1).get().getValue());
        Assert.assertNull(metadataStore.read(KEY2).get().getValue());
    }

    /**
     * Tests simple scenario with lazy commit.
     *
     * @throws Exception
     */
    @Test
    public void testLazyCommitWithNormalCommit() throws Exception {
        try (MetadataTransaction txn = metadataStore.beginTransaction(false, KEYS)) {
            // NO value should be found.
            assertNull(txn.get(KEY0));
            assertNull(txn.get(KEY1));
            assertNull(txn.get(KEY2));

            // Create data
            txn.create(new MockStorageMetadata(KEY0, VALUE0));
            txn.create(new MockStorageMetadata(KEY1, VALUE1));
            txn.create(new MockStorageMetadata(KEY2, VALUE2));

            assertEquals(txn.get(KEY0), KEY0, VALUE0);
            assertEquals(txn.get(KEY1), KEY1, VALUE1);
            assertEquals(txn.get(KEY2), KEY2, VALUE2);

            txn.commit().join();

        } catch (Exception e) {
            throw e;
        }

        // make sure they are stored correctly
        assertEquals((MockStorageMetadata) metadataStore.read(KEY0).get().getValue(), KEY0, VALUE0);
        assertEquals((MockStorageMetadata) metadataStore.read(KEY1).get().getValue(), KEY1, VALUE1);
        assertEquals((MockStorageMetadata) metadataStore.read(KEY2).get().getValue(), KEY2, VALUE2);

        try (MetadataTransaction txn1 = metadataStore.beginTransaction(false, KEYS)) {
            // Update
            txn1.update(new MockStorageMetadata(KEY1, VALUE4));
            assertEquals(txn1.get(KEY0), KEY0, VALUE0);
            assertEquals(txn1.get(KEY1), KEY1, VALUE4);
            assertEquals(txn1.get(KEY2), KEY2, VALUE2);

            // delete
            txn1.delete(KEY2);
            assertEquals(txn1.get(KEY0), KEY0, VALUE0);
            assertEquals(txn1.get(KEY1), KEY1, VALUE4);
            assertNull(txn1.get(KEY2));
            txn1.commit(true).join(); // Commit lazily.

            assertEquals(txn1.get(KEY0), KEY0, VALUE0);
            assertEquals(txn1.get(KEY1), KEY1, VALUE4);
            assertNull(txn1.get(KEY2));

        } catch (Exception e) {
            throw e;
        }

        // make sure the persistent store is not affected.
        assertEquals((MockStorageMetadata) metadataStore.read(KEY0).get().getValue(), KEY0, VALUE0);
        assertEquals((MockStorageMetadata) metadataStore.read(KEY1).get().getValue(), KEY1, VALUE1);
        assertEquals((MockStorageMetadata) metadataStore.read(KEY2).get().getValue(), KEY2, VALUE2);

        // See the effect of transaction after words in a new transaction.
        // Note this transaction is aborted. So any changes made here should be totally ignored.
        try (MetadataTransaction txn2 = metadataStore.beginTransaction(false, KEYS)) {
            assertEquals(txn2.get(KEY0), KEY0, VALUE0);
            assertEquals(txn2.get(KEY1), KEY1, VALUE4);
            assertNull(txn2.get(KEY2));

            txn2.update(new MockStorageMetadata(KEY0, VALUE6));
            txn2.delete(KEY1);
            // This is still
            assertEquals(txn2.get(KEY0), KEY0, VALUE6);
            assertNull(txn2.get(KEY3));
            assertNull(txn2.get(KEY1));
            // abort
            txn2.abort();
        } catch (Exception e) {
            throw e;
        }
        // Should have no effect;
        try (MetadataTransaction txn3 = metadataStore.beginTransaction(false, KEYS)) {
            assertEquals(txn3.get(KEY0), KEY0, VALUE0);
            assertEquals(txn3.get(KEY1), KEY1, VALUE4);
            assertNull(txn3.get(KEY2));
        } catch (Exception e) {
            throw e;
        }

        try (MetadataTransaction txn4 = metadataStore.beginTransaction(false, KEYS)) {
            // Update
            txn4.update(new MockStorageMetadata(KEY0, VALUE5));
            txn4.update(new MockStorageMetadata(KEY1, VALUE6));
            assertEquals(txn4.get(KEY0), KEY0, VALUE5);
            assertEquals(txn4.get(KEY1), KEY1, VALUE6);
            assertNull(txn4.get(KEY2));

            // delete
            txn4.delete(KEY1);
            assertEquals(txn4.get(KEY0), KEY0, VALUE5);
            assertNull(txn4.get(KEY1));
            assertNull(txn4.get(KEY2));
            txn4.commit().join();

            assertEquals(txn4.get(KEY0), KEY0, VALUE5);
            assertNull(txn4.get(KEY1));
            assertNull(txn4.get(KEY2));

        } catch (Exception e) {
            throw e;
        }

        // make sure the persistent store is not affected.
        assertEquals((MockStorageMetadata) metadataStore.read(KEY0).get().getValue(), KEY0, VALUE5);
        Assert.assertNull(metadataStore.read(KEY1).get().getValue());
        assertEquals((MockStorageMetadata) metadataStore.read(KEY2).get().getValue(), KEY2, VALUE2);
    }

    @Test
    public void testTransactionFailedForMultipleUpdates() throws Exception {
        // Step 1: Set up
        try (MetadataTransaction txn = metadataStore.beginTransaction(false, KEYS)) {
            assertNull(txn.get(KEY0));
            assertNull(txn.get(KEY1));
            assertNull(txn.get(KEY3));
            // Create data
            txn.create(new MockStorageMetadata(KEY0, VALUE0));
            txn.create(new MockStorageMetadata(KEY1, VALUE1));
            txn.create(new MockStorageMetadata(KEY3, VALUE2));

            assertEquals(txn.get(KEY0), KEY0, VALUE0);
            assertEquals(txn.get(KEY1), KEY1, VALUE1);
            assertEquals(txn.get(KEY3), KEY3, VALUE2);

            txn.commit(false).join();

            // Same data after commit.
            assertEquals(txn.get(KEY0), KEY0, VALUE0);
            assertEquals(txn.get(KEY1), KEY1, VALUE1);
            assertEquals(txn.get(KEY3), KEY3, VALUE2);

        } catch (Exception e) {
            throw e;
        }

        // Step 2: Create a test transaction
        try (MetadataTransaction txn0 = metadataStore.beginTransaction(false, KEYS)) {

            // Step 2 A: Make some updates, but don't commit yet.
            txn0.update(new MockStorageMetadata(KEY0, VALUE5));
            assertEquals(txn0.get(KEY0), KEY0, VALUE5);
            assertEquals(txn0.get(KEY3), KEY3, VALUE2);

            // Step 3 : Start a parallel transaction
            try (MetadataTransaction txn2 = metadataStore.beginTransaction(false, KEYS)) {
                // Step 3 A: Make some updates, but don't commit yet.
                txn2.update(new MockStorageMetadata(KEY0, VALUE6));
                assertEquals(txn2.get(KEY0), KEY0, VALUE6);
                assertEquals(txn2.get(KEY3), KEY3, VALUE2);

                // Step 3 B: Commit. It should succeed.
                txn2.commit(false).join();
                assertEquals(txn2.get(KEY0), KEY0, VALUE6);
                assertEquals(txn2.get(KEY3), KEY3, VALUE2);

            } catch (Exception e) {
                throw e;
            }

            // Step 4 : Start a second parallel transaction
            try (MetadataTransaction txn2 = metadataStore.beginTransaction(false, KEYS)) {
                // Step 4 : Data committed by earlier transaction should be visible to other new transactions
                assertEquals(txn2.get(KEY0), KEY0, VALUE6);
                assertEquals(txn2.get(KEY3), KEY3, VALUE2);
            } catch (Exception e) {
                throw e;
            }

            // A committed transactions should not have any effect on this transaction.
            assertEquals(txn0.get(KEY0), KEY0, VALUE5);
            assertEquals(txn0.get(KEY3), KEY3, VALUE2);

            try {
                txn0.commit(false).join();
                Assert.fail("Transaction should be aborted.");
            } catch (CompletionException e) {
                Assert.assertTrue(e.getCause() instanceof StorageMetadataVersionMismatchException);
            }
        }
    }

    @Test
    public void testTransactionFailedForMultipleOperations() throws Exception {

        // Set up the data there are 3 keys
        try (MetadataTransaction txn = metadataStore.beginTransaction(false, KEYS)) {
            assertNull(txn.get(KEY0));
            assertNull(txn.get(KEY1));
            assertNull(txn.get(KEY3));
            // Create data
            txn.create(new MockStorageMetadata(KEY0, VALUE0));
            txn.create(new MockStorageMetadata(KEY1, VALUE1));
            txn.create(new MockStorageMetadata(KEY3, VALUE2));

            assertEquals(txn.get(KEY0), KEY0, VALUE0);
            assertEquals(txn.get(KEY1), KEY1, VALUE1);
            assertEquals(txn.get(KEY3), KEY3, VALUE2);

            txn.commit(false).join();

            // Same data after commit.
            assertEquals(txn.get(KEY0), KEY0, VALUE0);
            assertEquals(txn.get(KEY1), KEY1, VALUE1);
            assertEquals(txn.get(KEY3), KEY3, VALUE2);

        } catch (Exception e) {
            throw e;
        }

        // Start a test transaction
        try (MetadataTransaction txn0 = metadataStore.beginTransaction(false, KEYS)) {
            // Update/delete some keys but don't commit .
            // We wan to see that these changes do not affect other transactions
            txn0.update(new MockStorageMetadata(KEY0, VALUE5));
            txn0.delete(KEY1);
            assertEquals(txn0.get(KEY0), KEY0, VALUE5);
            assertEquals(txn0.get(KEY3), KEY3, VALUE2);
            assertNull(txn0.get(KEY1));

            // Another Transaction that must be able modify its view data and commit
            try (MetadataTransaction txn2 = metadataStore.beginTransaction(false, KEYS)) {

                txn2.update(new MockStorageMetadata(KEY0, VALUE6));
                txn2.delete(KEY1);
                assertEquals(txn2.get(KEY0), KEY0, VALUE6);
                assertEquals(txn2.get(KEY3), KEY3, VALUE2);
                assertNull(txn2.get(KEY1));

                txn2.commit(false).join();
                assertEquals(txn2.get(KEY0), KEY0, VALUE6);
                assertEquals(txn2.get(KEY3), KEY3, VALUE2);
                assertNull(txn2.get(KEY1));
            } catch (Exception e) {
                throw e;
            }

            // Yet another Transaction that must be able modify its view data and commit
            try (MetadataTransaction txn2 = metadataStore.beginTransaction(false, KEYS)) {

                assertEquals(txn2.get(KEY0), KEY0, VALUE6);
                assertEquals(txn2.get(KEY3), KEY3, VALUE2);
                assertNull(txn2.get(KEY1));
            } catch (Exception e) {
                throw e;
            }

            // A committed transaction should not have any effect on this transaction.
            assertEquals(txn0.get(KEY0), KEY0, VALUE5);
            assertEquals(txn0.get(KEY3), KEY3, VALUE2);
            assertNull(txn0.get(KEY1));

            // However when it tries to commit this should fail.
            try {
                txn0.commit(false).join();
                Assert.fail("Transaction should be aborted.");
            } catch (CompletionException e) {
                Assert.assertTrue(e.getCause() instanceof StorageMetadataVersionMismatchException);
            }
        }
    }

    /**
     * Test that only one of the concurrent commit can proceed and others fail.
     *
     * @throws Exception Exception if any.
     */
    @Test
    public void testCommitSafety() throws Exception {
        if (metadataStore instanceof InMemoryMetadataStore) {
            // Set up the callback so that you can block writeAll call, to create situation where commit is in flight but not finished.
            final CompletableFuture<Void> futureToBlockOn = new CompletableFuture<Void>();
            ((InMemoryMetadataStore) metadataStore).setWriteCallback( v -> futureToBlockOn.thenApplyAsync( v2 -> null, executorService()));
            CompletableFuture<Void> commitFuture;
            try (MetadataTransaction txn = metadataStore.beginTransaction(false, KEYS)) {
                assertNull(txn.get(KEY0));
                txn.create(new MockStorageMetadata(KEY0, VALUE0));
                Assert.assertNotNull(txn.get(KEY0));
                commitFuture = txn.commit(); // Do not wait , this call should block
            }

            try {
                // Try commit on all keys
                try (MetadataTransaction txn = metadataStore.beginTransaction(false, KEYS)) {
                    assertNull(txn.get(KEY0));
                    txn.create(new MockStorageMetadata(KEY0, VALUE1));
                    assertEquals(txn.get(KEY0), KEY0, VALUE1);
                    AssertExtensions.assertFutureThrows(
                            "Transaction should fail.",
                            txn.commit(),
                            ex -> ex instanceof StorageMetadataVersionMismatchException);
                }

                // Try commit only on only one key.
                try (MetadataTransaction txn = metadataStore.beginTransaction(false, KEY0)) {
                    assertNull(txn.get(KEY0));
                    txn.create(new MockStorageMetadata(KEY0, VALUE1));
                    assertEquals(txn.get(KEY0), KEY0, VALUE1);
                    AssertExtensions.assertFutureThrows(
                            "Transaction should fail.",
                            txn.commit(),
                            ex -> ex instanceof StorageMetadataVersionMismatchException);
                }
            } finally {
                futureToBlockOn.complete(null);
            }
            commitFuture.join();
            // make sure the valid transaction went through.
            try (MetadataTransaction txn = metadataStore.beginTransaction(false, KEY0)) {
                assertEquals(txn.get(KEY0), KEY0, VALUE0);
            }
        }
    }

    /**
     * Test that only one of the concurrent commit can proceed and others fail.
     *
     * @throws Exception Exception if any.
     */
    @Test
    public void testCommitSafetyWithSubset() throws Exception {
        if (metadataStore instanceof InMemoryMetadataStore) {
            // Set up the callback so that you can block writeAll call, to create situation where commit is in flight but not finished.
            final CompletableFuture<Void> futureToBlockOn = new CompletableFuture<Void>();
            ((InMemoryMetadataStore) metadataStore).setWriteCallback( transactionDataList -> {
                // Block only on KEY0.
                if (transactionDataList.stream().filter(t -> t.getKey().equals(KEY0)).findAny().isPresent()) {
                    return futureToBlockOn.thenApplyAsync(v2 -> null, executorService());
                } else {
                    return CompletableFuture.completedFuture(null);
                }
            });
            CompletableFuture<Void> commitFuture;
            try (MetadataTransaction txn = metadataStore.beginTransaction(false, KEY0, KEY1)) {
                assertNull(txn.get(KEY0));
                txn.create(new MockStorageMetadata(KEY0, VALUE0));
                Assert.assertNotNull(txn.get(KEY0));
                commitFuture = txn.commit(); // Do not wait , this call should block
            }

            try {
                // Try commit on all keys
                try (MetadataTransaction txn = metadataStore.beginTransaction(false, KEYS)) {
                    assertNull(txn.get(KEY0));
                    txn.create(new MockStorageMetadata(KEY0, VALUE1));
                    assertEquals(txn.get(KEY0), KEY0, VALUE1);
                    AssertExtensions.assertFutureThrows(
                            "Transaction should fail.",
                            txn.commit(),
                            ex -> ex instanceof StorageMetadataVersionMismatchException);
                }

                // Try commit only on only one key.
                try (MetadataTransaction txn = metadataStore.beginTransaction(false, KEY0)) {
                    assertNull(txn.get(KEY0));
                    txn.create(new MockStorageMetadata(KEY0, VALUE1));
                    assertEquals(txn.get(KEY0), KEY0, VALUE1);
                    AssertExtensions.assertFutureThrows(
                            "Transaction should fail.",
                            txn.commit(),
                            ex -> ex instanceof StorageMetadataVersionMismatchException);
                }

                // Try commit only on only one key.
                try (MetadataTransaction txn = metadataStore.beginTransaction(false, KEY1)) {
                    assertNull(txn.get(KEY0));
                    txn.create(new MockStorageMetadata(KEY0, VALUE1));
                    assertEquals(txn.get(KEY0), KEY0, VALUE1);
                    AssertExtensions.assertFutureThrows(
                            "Transaction should fail.",
                            txn.commit(),
                            ex -> ex instanceof StorageMetadataVersionMismatchException);
                }
                // Make sure transactions on unrelated keys works.
                try (MetadataTransaction txn = metadataStore.beginTransaction(false, KEY4)) {
                    assertNull(txn.get(KEY4));
                    txn.create(new MockStorageMetadata(KEY4, VALUE4));
                    assertEquals(txn.get(KEY4), KEY4, VALUE4);
                    txn.commit().join();
                }
                try (MetadataTransaction txn = metadataStore.beginTransaction(true, KEY4)) {
                    assertEquals(txn.get(KEY4), KEY4, VALUE4);
                }
            } finally {
                futureToBlockOn.complete(null);
            }
            commitFuture.join();
            // make sure the valid transaction went through.
            try (MetadataTransaction txn = metadataStore.beginTransaction(false, KEY0)) {
                assertEquals(txn.get(KEY0), KEY0, VALUE0);
            }
        }
    }

    @Test
    public void testEvictionFromBuffer() throws Exception {
        if (metadataStore instanceof InMemoryMetadataStore) {
            metadataStore.setMaxEntriesInCache(10);
            metadataStore.setMaxEntriesInTxnBuffer(10);
            for (int i = 0; i < 10000; i++) {
                try (MetadataTransaction txn = metadataStore.beginTransaction(false, "Txn" + i)) {
                    txn.create(new MockStorageMetadata("Txn" + i, "Value" + i));
                    txn.commit().get();
                }
                try (MetadataTransaction txn = metadataStore.beginTransaction(false, "Txn" + i)) {
                    txn.delete("Txn" + i);
                    txn.commit().get();
                }
            }
            Assert.assertTrue(metadataStore.getBufferCount() < 10);
        }
    }

    private void assertNotNull(CompletableFuture<StorageMetadata> data) throws Exception {
        Assert.assertNotNull(data.get());
    }

    private void assertNull(CompletableFuture<StorageMetadata> data) throws Exception {
        Assert.assertNull(data.get());
    }

    private void assertEquals(CompletableFuture<StorageMetadata> data, String key, String value) throws Exception {
        assertEquals((MockStorageMetadata) data.get(), key, value);
    }

    private void assertEquals(MockStorageMetadata data, String key, String value) {
        Assert.assertEquals("Should get the same key", key, data.getKey());
        Assert.assertEquals("Should get the same data", value, data.getValue());
    }

    private static void assertEquals(MockStorageMetadata expected, CompletableFuture<StorageMetadata> actual) throws Exception {
        assertEquals(expected, (MockStorageMetadata) actual.get());
    }

    private static void assertEquals(MockStorageMetadata expected, MockStorageMetadata actual) {
        Assert.assertEquals(expected.getKey(), actual.getKey());
        Assert.assertEquals(expected.getValue(), actual.getValue());
    }

}
