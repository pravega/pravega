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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for ChunkMetadataStoreTests.
 */
public class ChunkMetadataStoreTests {

    public static final String KEY0 = "donald";
    public static final String KEY2 = "goofy";
    public static final String KEY1 = "micky";
    public static final String KEY3 = "pluto";
    public static final String KEY4 = "mini";

    public static final String VALUE0 = "duck";
    public static final String VALUE1 = "rat";
    public static final String VALUE2 = "dog";
    public static final String VALUE4 = "mouse";
    public static final String VALUE5 = "avian";
    public static final String VALUE6 = "bird";

    @Rule
    public Timeout globalTimeout = Timeout.seconds(300);

    protected BaseMetadataStore metadataStore;

    @Before
    public void setUp() throws Exception {
        metadataStore = new InMemoryMetadataStore();
    }

    @After
    public void tearDown() {
    }

    /**
     * Test basic invariants
     *
     * @throws Exception Exception if any.
     */
    @Test
    public void testInvariants() throws Exception {
        try (MetadataTransaction txn = metadataStore.beginTransaction()) {
            Assert.assertFalse(txn.isAborted());
            Assert.assertFalse(txn.isCommitted());
            Assert.assertNull(txn.get(null));

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
        try (MetadataTransaction txn = metadataStore.beginTransaction()) {
            Assert.assertNull(txn.get(KEY0));
            txn.create(new MockStorageMetadata(KEY0, VALUE0));
            Assert.assertNotNull(txn.get(KEY0));
            txn.delete(KEY0);
            Assert.assertNull(txn.get(KEY0));
            txn.commit();
        }

        try (MetadataTransaction txn = metadataStore.beginTransaction()) {
            Assert.assertNull(txn.get(KEY0));
            // Should be able to recreate
            txn.create(new MockStorageMetadata(KEY0, VALUE0));
            assertEquals((MockStorageMetadata) txn.get(KEY0), KEY0, VALUE0);
            txn.commit();
        }

        try (MetadataTransaction txn = metadataStore.beginTransaction()) {
            assertEquals((MockStorageMetadata) txn.get(KEY0), KEY0, VALUE0);
        }
    }

    /**
     * Test a simple scenario.
     *
     * @throws Exception Exception if any.
     */
    @Test
    public void testSimpleScenario() throws Exception {
        try (MetadataTransaction txn = metadataStore.beginTransaction()) {
            // NO value should be found.
            Assert.assertNull(txn.get(KEY0));
            Assert.assertNull(txn.get(KEY1));
            Assert.assertNull(txn.get(KEY3));

            // Create data
            txn.create(new MockStorageMetadata(KEY0, VALUE0));
            txn.create(new MockStorageMetadata(KEY1, VALUE1));
            txn.create(new MockStorageMetadata(KEY2, VALUE2));

            assertEquals((MockStorageMetadata) txn.get(KEY0), KEY0, VALUE0);
            assertEquals((MockStorageMetadata) txn.get(KEY1), KEY1, VALUE1);
            assertEquals((MockStorageMetadata) txn.get(KEY2), KEY2, VALUE2);

            // Update
            txn.update(new MockStorageMetadata(KEY1, VALUE4));
            assertEquals((MockStorageMetadata) txn.get(KEY0), KEY0, VALUE0);
            assertEquals((MockStorageMetadata) txn.get(KEY1), KEY1, VALUE4);
            assertEquals((MockStorageMetadata) txn.get(KEY2), KEY2, VALUE2);

            // create
            txn.create(new MockStorageMetadata(KEY3, VALUE5));
            assertEquals((MockStorageMetadata) txn.get(KEY0), KEY0, VALUE0);
            assertEquals((MockStorageMetadata) txn.get(KEY1), KEY1, VALUE4);
            assertEquals((MockStorageMetadata) txn.get(KEY2), KEY2, VALUE2);
            assertEquals((MockStorageMetadata) txn.get(KEY3), KEY3, VALUE5);

            // delete
            txn.delete(KEY2);
            txn.delete(KEY3);
            assertEquals((MockStorageMetadata) txn.get(KEY0), KEY0, VALUE0);
            assertEquals((MockStorageMetadata) txn.get(KEY1), KEY1, VALUE4);
            Assert.assertNull(txn.get(KEY3));
            txn.commit();

            assertEquals((MockStorageMetadata) txn.get(KEY0), KEY0, VALUE0);
            assertEquals((MockStorageMetadata) txn.get(KEY1), KEY1, VALUE4);
            Assert.assertNull(txn.get(KEY2));
            Assert.assertNull(txn.get(KEY3));
        } catch (Exception e) {
            throw e;
        }

        // make sure they are stored correctly
        assertEquals((MockStorageMetadata) metadataStore.read(KEY0).getValue(), KEY0, VALUE0);
        assertEquals((MockStorageMetadata) metadataStore.read(KEY1).getValue(), KEY1, VALUE4);
        Assert.assertNull(metadataStore.read(KEY2).getValue());
        Assert.assertNull(metadataStore.read(KEY3).getValue());

        // See the effect of transaction after words
        try (MetadataTransaction txn2 = metadataStore.beginTransaction()) {
            assertEquals((MockStorageMetadata) txn2.get(KEY0), KEY0, VALUE0);
            assertEquals((MockStorageMetadata) txn2.get(KEY1), KEY1, VALUE4);
            Assert.assertNull(txn2.get(KEY3));

            txn2.update(new MockStorageMetadata(KEY0, VALUE6));
            txn2.delete(KEY1);
            assertEquals((MockStorageMetadata) txn2.get(KEY0), KEY0, VALUE6);
            Assert.assertNull(txn2.get(KEY3));
            Assert.assertNull(txn2.get(KEY1));
            // Implicitly aborted
        } catch (Exception e) {
            throw e;
        }

        // Should have no effect;
        try (MetadataTransaction txn3 = metadataStore.beginTransaction()) {
            assertEquals((MockStorageMetadata) txn3.get(KEY0), KEY0, VALUE0);
            assertEquals((MockStorageMetadata) txn3.get(KEY1), KEY1, VALUE4);
            Assert.assertNull(txn3.get(KEY2));
            Assert.assertNull(txn3.get(KEY3));
        } catch (Exception e) {
            throw e;
        }

        // make sure they are stored correctly
        assertEquals((MockStorageMetadata) metadataStore.read(KEY0).getValue(), KEY0, VALUE0);
        assertEquals((MockStorageMetadata) metadataStore.read(KEY1).getValue(), KEY1, VALUE4);
        Assert.assertNull(metadataStore.read(KEY2).getValue());
        Assert.assertNull(metadataStore.read(KEY3).getValue());
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
        try (MetadataTransaction txn = metadataStore.beginTransaction()) {
            // NO value should be found.
            Assert.assertNull(txn.get(KEY0));
            Assert.assertNull(txn.get(KEY1));
            Assert.assertNull(txn.get(KEY2));

            // Create KEY0, KEY1, KEY2
            txn.create(new MockStorageMetadata(KEY0, VALUE0));
            txn.create(new MockStorageMetadata(KEY1, VALUE1));
            txn.create(new MockStorageMetadata(KEY2, VALUE2));

            assertEquals((MockStorageMetadata) txn.get(KEY0), KEY0, VALUE0);
            assertEquals((MockStorageMetadata) txn.get(KEY1), KEY1, VALUE1);
            assertEquals((MockStorageMetadata) txn.get(KEY2), KEY2, VALUE2);

            // Update KEY1
            txn.update(new MockStorageMetadata(KEY1, VALUE4));
            assertEquals((MockStorageMetadata) txn.get(KEY0), KEY0, VALUE0);
            assertEquals((MockStorageMetadata) txn.get(KEY1), KEY1, VALUE4);
            assertEquals((MockStorageMetadata) txn.get(KEY2), KEY2, VALUE2);

            // create KEY3
            txn.create(new MockStorageMetadata(KEY3, VALUE5));
            assertEquals((MockStorageMetadata) txn.get(KEY0), KEY0, VALUE0);
            assertEquals((MockStorageMetadata) txn.get(KEY1), KEY1, VALUE4);
            assertEquals((MockStorageMetadata) txn.get(KEY2), KEY2, VALUE2);
            assertEquals((MockStorageMetadata) txn.get(KEY3), KEY3, VALUE5);

            // delete KEY2, KEY3
            txn.delete(KEY2);
            txn.delete(KEY3);
            assertEquals((MockStorageMetadata) txn.get(KEY0), KEY0, VALUE0);
            assertEquals((MockStorageMetadata) txn.get(KEY1), KEY1, VALUE4);
            Assert.assertNull(txn.get(KEY2));
            Assert.assertNull(txn.get(KEY3));
            txn.commit(true);

            assertEquals((MockStorageMetadata) txn.get(KEY0), KEY0, VALUE0);
            assertEquals((MockStorageMetadata) txn.get(KEY1), KEY1, VALUE4);
            Assert.assertNull(txn.get(KEY2));
            Assert.assertNull(txn.get(KEY3));

            // make sure they are stored correctly
            Assert.assertNull(metadataStore.read(KEY0).getValue());
            Assert.assertNull(metadataStore.read(KEY1).getValue());
            Assert.assertNull(metadataStore.read(KEY2).getValue());
            Assert.assertNull(metadataStore.read(KEY3).getValue());

        } catch (Exception e) {
            throw e;
        }

        // See the effect of transaction after words
        try (MetadataTransaction txn2 = metadataStore.beginTransaction()) {
            assertEquals((MockStorageMetadata) txn2.get(KEY0), KEY0, VALUE0);
            assertEquals((MockStorageMetadata) txn2.get(KEY1), KEY1, VALUE4);
            Assert.assertNull(txn2.get(KEY3));

            txn2.update(new MockStorageMetadata(KEY0, VALUE6));
            txn2.delete(KEY1);
            assertEquals((MockStorageMetadata) txn2.get(KEY0), KEY0, VALUE6);
            Assert.assertNull(txn2.get(KEY3));
            Assert.assertNull(txn2.get(KEY1));
            // Implicitly aborted
        } catch (Exception e) {
            throw e;
        }

        // Should have no effect;
        try (MetadataTransaction txn3 = metadataStore.beginTransaction()) {
            assertEquals((MockStorageMetadata) txn3.get(KEY0), KEY0, VALUE0);
            assertEquals((MockStorageMetadata) txn3.get(KEY1), KEY1, VALUE4);
            Assert.assertNull(txn3.get(KEY3));
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
        try (MetadataTransaction txn = metadataStore.beginTransaction()) {
            // NO value should be found.
            Assert.assertNull(txn.get(KEY0));
            Assert.assertNull(txn.get(KEY1));
            Assert.assertNull(txn.get(KEY2));

            // Create KEY0, KEY1, KEY2
            txn.create(new MockStorageMetadata(KEY0, VALUE0));
            txn.create(new MockStorageMetadata(KEY1, VALUE1));
            txn.create(new MockStorageMetadata(KEY2, VALUE2));
            assertEquals((MockStorageMetadata) txn.get(KEY0), KEY0, VALUE0);
            assertEquals((MockStorageMetadata) txn.get(KEY1), KEY1, VALUE1);
            assertEquals((MockStorageMetadata) txn.get(KEY2), KEY2, VALUE2);

            txn.markPinned(txn.get(KEY0));
            txn.markPinned(txn.get(KEY1));
            txn.markPinned(txn.get(KEY2));

            // Update KEY1
            txn.update(new MockStorageMetadata(KEY1, VALUE4));
            assertEquals((MockStorageMetadata) txn.get(KEY0), KEY0, VALUE0);
            assertEquals((MockStorageMetadata) txn.get(KEY1), KEY1, VALUE4);
            assertEquals((MockStorageMetadata) txn.get(KEY2), KEY2, VALUE2);

            // create KEY3
            txn.create(new MockStorageMetadata(KEY3, VALUE5));
            assertEquals((MockStorageMetadata) txn.get(KEY0), KEY0, VALUE0);
            assertEquals((MockStorageMetadata) txn.get(KEY1), KEY1, VALUE4);
            assertEquals((MockStorageMetadata) txn.get(KEY2), KEY2, VALUE2);
            assertEquals((MockStorageMetadata) txn.get(KEY3), KEY3, VALUE5);

            // delete KEY2, KEY3
            txn.delete(KEY2);
            txn.delete(KEY3);
            assertEquals((MockStorageMetadata) txn.get(KEY0), KEY0, VALUE0);
            assertEquals((MockStorageMetadata) txn.get(KEY1), KEY1, VALUE4);
            Assert.assertNull(txn.get(KEY2));
            Assert.assertNull(txn.get(KEY3));
            txn.commit();

            assertEquals((MockStorageMetadata) txn.get(KEY0), KEY0, VALUE0);
            assertEquals((MockStorageMetadata) txn.get(KEY1), KEY1, VALUE4);
            Assert.assertNull(txn.get(KEY2));
            Assert.assertNull(txn.get(KEY3));

            // make sure they are stored correctly
            Assert.assertNull(metadataStore.read(KEY0).getValue());
            Assert.assertNull(metadataStore.read(KEY1).getValue());
            Assert.assertNull(metadataStore.read(KEY2).getValue());
            Assert.assertNull(metadataStore.read(KEY3).getValue());

        } catch (Exception e) {
            throw e;
        }

        // See the effect of transaction after words
        try (MetadataTransaction txn2 = metadataStore.beginTransaction()) {
            assertEquals((MockStorageMetadata) txn2.get(KEY0), KEY0, VALUE0);
            assertEquals((MockStorageMetadata) txn2.get(KEY1), KEY1, VALUE4);
            Assert.assertNull(txn2.get(KEY3));

            txn2.update(new MockStorageMetadata(KEY0, VALUE6));
            txn2.delete(KEY1);
            assertEquals((MockStorageMetadata) txn2.get(KEY0), KEY0, VALUE6);
            Assert.assertNull(txn2.get(KEY3));
            Assert.assertNull(txn2.get(KEY1));
            // Implicitly aborted
        } catch (Exception e) {
            throw e;
        }

        // Should have no effect;
        try (MetadataTransaction txn3 = metadataStore.beginTransaction()) {
            assertEquals((MockStorageMetadata) txn3.get(KEY0), KEY0, VALUE0);
            assertEquals((MockStorageMetadata) txn3.get(KEY1), KEY1, VALUE4);
            Assert.assertNull(txn3.get(KEY3));
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
        try (MetadataTransaction txn = metadataStore.beginTransaction()) {
            // NO value should be found.
            Assert.assertNull(txn.get(KEY0));
            Assert.assertNull(txn.get(KEY1));
            Assert.assertNull(txn.get(KEY2));

            // Create data
            txn.create(new MockStorageMetadata(KEY0, VALUE0));
            txn.create(new MockStorageMetadata(KEY1, VALUE1));
            txn.create(new MockStorageMetadata(KEY2, VALUE2));

            assertEquals((MockStorageMetadata) txn.get(KEY0), KEY0, VALUE0);
            assertEquals((MockStorageMetadata) txn.get(KEY1), KEY1, VALUE1);
            assertEquals((MockStorageMetadata) txn.get(KEY2), KEY2, VALUE2);

            txn.commit(); // This is normal commit.

        } catch (Exception e) {
            throw e;
        }

        // make sure they are stored correctly
        assertEquals((MockStorageMetadata) metadataStore.read(KEY0).getValue(), KEY0, VALUE0);
        assertEquals((MockStorageMetadata) metadataStore.read(KEY1).getValue(), KEY1, VALUE1);
        assertEquals((MockStorageMetadata) metadataStore.read(KEY2).getValue(), KEY2, VALUE2);

        try (MetadataTransaction txn1 = metadataStore.beginTransaction()) {
            // Update
            txn1.update(new MockStorageMetadata(KEY1, VALUE4));
            assertEquals((MockStorageMetadata) txn1.get(KEY0), KEY0, VALUE0);
            assertEquals((MockStorageMetadata) txn1.get(KEY1), KEY1, VALUE4);
            assertEquals((MockStorageMetadata) txn1.get(KEY2), KEY2, VALUE2);

            // delete
            txn1.delete(KEY2);
            assertEquals((MockStorageMetadata) txn1.get(KEY0), KEY0, VALUE0);
            assertEquals((MockStorageMetadata) txn1.get(KEY1), KEY1, VALUE4);
            Assert.assertNull(txn1.get(KEY2));
            txn1.commit(true); // Commit lazily.

            assertEquals((MockStorageMetadata) txn1.get(KEY0), KEY0, VALUE0);
            assertEquals((MockStorageMetadata) txn1.get(KEY1), KEY1, VALUE4);
            Assert.assertNull(txn1.get(KEY2));

        } catch (Exception e) {
            throw e;
        }

        // make sure the persistent store actually has the correct value.
        assertEquals((MockStorageMetadata) metadataStore.read(KEY0).getValue(), KEY0, VALUE0);
        assertEquals((MockStorageMetadata) metadataStore.read(KEY1).getValue(), KEY1, VALUE4);
        Assert.assertNull(metadataStore.read(KEY2).getValue());

        // See the effect of transaction after words in a new transaction.
        // Note this transaction is aborted. So any changes made here should be totally ignored.
        try (MetadataTransaction txn2 = metadataStore.beginTransaction()) {
            assertEquals((MockStorageMetadata) txn2.get(KEY0), KEY0, VALUE0);
            assertEquals((MockStorageMetadata) txn2.get(KEY1), KEY1, VALUE4);
            Assert.assertNull(txn2.get(KEY2));

            txn2.update(new MockStorageMetadata(KEY0, VALUE6));
            txn2.delete(KEY1);
            // This is still
            assertEquals((MockStorageMetadata) txn2.get(KEY0), KEY0, VALUE6);
            Assert.assertNull(txn2.get(KEY3));
            Assert.assertNull(txn2.get(KEY1));
            // abort
            txn2.abort();
        } catch (Exception e) {
            throw e;
        }
        // Should have no effect;
        try (MetadataTransaction txn3 = metadataStore.beginTransaction()) {
            assertEquals((MockStorageMetadata) txn3.get(KEY0), KEY0, VALUE0);
            assertEquals((MockStorageMetadata) txn3.get(KEY1), KEY1, VALUE4);
            Assert.assertNull(txn3.get(KEY2));
        } catch (Exception e) {
            throw e;
        }

        try (MetadataTransaction txn4 = metadataStore.beginTransaction()) {
            // Update
            txn4.update(new MockStorageMetadata(KEY0, VALUE5));
            txn4.update(new MockStorageMetadata(KEY1, VALUE6));
            assertEquals((MockStorageMetadata) txn4.get(KEY0), KEY0, VALUE5);
            assertEquals((MockStorageMetadata) txn4.get(KEY1), KEY1, VALUE6);
            Assert.assertNull(txn4.get(KEY2));

            // delete
            txn4.delete(KEY1);
            assertEquals((MockStorageMetadata) txn4.get(KEY0), KEY0, VALUE5);
            Assert.assertNull(txn4.get(KEY1));
            Assert.assertNull(txn4.get(KEY2));
            txn4.commit();

            assertEquals((MockStorageMetadata) txn4.get(KEY0), KEY0, VALUE5);
            Assert.assertNull(txn4.get(KEY1));
            Assert.assertNull(txn4.get(KEY2));

        } catch (Exception e) {
            throw e;
        }

        // make sure the persistent store is not affected.
        assertEquals((MockStorageMetadata) metadataStore.read(KEY0).getValue(), KEY0, VALUE5);
        Assert.assertNull(metadataStore.read(KEY1).getValue());
        Assert.assertNull(metadataStore.read(KEY2).getValue());
    }

    /**
     * Tests simple scenario with lazy commit.
     *
     * @throws Exception
     */
    @Test
    public void testLazyCommitWithNormalCommit() throws Exception {
        try (MetadataTransaction txn = metadataStore.beginTransaction()) {
            // NO value should be found.
            Assert.assertNull(txn.get(KEY0));
            Assert.assertNull(txn.get(KEY1));
            Assert.assertNull(txn.get(KEY2));

            // Create data
            txn.create(new MockStorageMetadata(KEY0, VALUE0));
            txn.create(new MockStorageMetadata(KEY1, VALUE1));
            txn.create(new MockStorageMetadata(KEY2, VALUE2));

            assertEquals((MockStorageMetadata) txn.get(KEY0), KEY0, VALUE0);
            assertEquals((MockStorageMetadata) txn.get(KEY1), KEY1, VALUE1);
            assertEquals((MockStorageMetadata) txn.get(KEY2), KEY2, VALUE2);

            txn.commit();

        } catch (Exception e) {
            throw e;
        }

        // make sure they are stored correctly
        assertEquals((MockStorageMetadata) metadataStore.read(KEY0).getValue(), KEY0, VALUE0);
        assertEquals((MockStorageMetadata) metadataStore.read(KEY1).getValue(), KEY1, VALUE1);
        assertEquals((MockStorageMetadata) metadataStore.read(KEY2).getValue(), KEY2, VALUE2);

        try (MetadataTransaction txn1 = metadataStore.beginTransaction()) {
            // Update
            txn1.update(new MockStorageMetadata(KEY1, VALUE4));
            assertEquals((MockStorageMetadata) txn1.get(KEY0), KEY0, VALUE0);
            assertEquals((MockStorageMetadata) txn1.get(KEY1), KEY1, VALUE4);
            assertEquals((MockStorageMetadata) txn1.get(KEY2), KEY2, VALUE2);

            // delete
            txn1.delete(KEY2);
            assertEquals((MockStorageMetadata) txn1.get(KEY0), KEY0, VALUE0);
            assertEquals((MockStorageMetadata) txn1.get(KEY1), KEY1, VALUE4);
            Assert.assertNull(txn1.get(KEY2));
            txn1.commit(true); // Commit lazily.

            assertEquals((MockStorageMetadata) txn1.get(KEY0), KEY0, VALUE0);
            assertEquals((MockStorageMetadata) txn1.get(KEY1), KEY1, VALUE4);
            Assert.assertNull(txn1.get(KEY2));

        } catch (Exception e) {
            throw e;
        }

        // make sure the persistent store is not affected.
        assertEquals((MockStorageMetadata) metadataStore.read(KEY0).getValue(), KEY0, VALUE0);
        assertEquals((MockStorageMetadata) metadataStore.read(KEY1).getValue(), KEY1, VALUE1);
        assertEquals((MockStorageMetadata) metadataStore.read(KEY2).getValue(), KEY2, VALUE2);

        // See the effect of transaction after words in a new transaction.
        // Note this transaction is aborted. So any changes made here should be totally ignored.
        try (MetadataTransaction txn2 = metadataStore.beginTransaction()) {
            assertEquals((MockStorageMetadata) txn2.get(KEY0), KEY0, VALUE0);
            assertEquals((MockStorageMetadata) txn2.get(KEY1), KEY1, VALUE4);
            Assert.assertNull(txn2.get(KEY2));

            txn2.update(new MockStorageMetadata(KEY0, VALUE6));
            txn2.delete(KEY1);
            // This is still
            assertEquals((MockStorageMetadata) txn2.get(KEY0), KEY0, VALUE6);
            Assert.assertNull(txn2.get(KEY3));
            Assert.assertNull(txn2.get(KEY1));
            // abort
            txn2.abort();
        } catch (Exception e) {
            throw e;
        }
        // Should have no effect;
        try (MetadataTransaction txn3 = metadataStore.beginTransaction()) {
            assertEquals((MockStorageMetadata) txn3.get(KEY0), KEY0, VALUE0);
            assertEquals((MockStorageMetadata) txn3.get(KEY1), KEY1, VALUE4);
            Assert.assertNull(txn3.get(KEY2));
        } catch (Exception e) {
            throw e;
        }

        try (MetadataTransaction txn4 = metadataStore.beginTransaction()) {
            // Update
            txn4.update(new MockStorageMetadata(KEY0, VALUE5));
            txn4.update(new MockStorageMetadata(KEY1, VALUE6));
            assertEquals((MockStorageMetadata) txn4.get(KEY0), KEY0, VALUE5);
            assertEquals((MockStorageMetadata) txn4.get(KEY1), KEY1, VALUE6);
            Assert.assertNull(txn4.get(KEY2));

            // delete
            txn4.delete(KEY1);
            assertEquals((MockStorageMetadata) txn4.get(KEY0), KEY0, VALUE5);
            Assert.assertNull(txn4.get(KEY1));
            Assert.assertNull(txn4.get(KEY2));
            txn4.commit();

            assertEquals((MockStorageMetadata) txn4.get(KEY0), KEY0, VALUE5);
            Assert.assertNull(txn4.get(KEY1));
            Assert.assertNull(txn4.get(KEY2));

        } catch (Exception e) {
            throw e;
        }

        // make sure the persistent store is not affected.
        assertEquals((MockStorageMetadata) metadataStore.read(KEY0).getValue(), KEY0, VALUE5);
        Assert.assertNull(metadataStore.read(KEY1).getValue());
        assertEquals((MockStorageMetadata) metadataStore.read(KEY2).getValue(), KEY2, VALUE2);
    }

    @Test
    public void testTransactionFailedForMultipleUpdates() throws Exception {
        // Step 1: Set up
        try (MetadataTransaction txn = metadataStore.beginTransaction()) {
            Assert.assertNull(txn.get(KEY0));
            Assert.assertNull(txn.get(KEY1));
            Assert.assertNull(txn.get(KEY3));
            // Create data
            txn.create(new MockStorageMetadata(KEY0, VALUE0));
            txn.create(new MockStorageMetadata(KEY1, VALUE1));
            txn.create(new MockStorageMetadata(KEY3, VALUE2));

            assertEquals((MockStorageMetadata) txn.get(KEY0), KEY0, VALUE0);
            assertEquals((MockStorageMetadata) txn.get(KEY1), KEY1, VALUE1);
            assertEquals((MockStorageMetadata) txn.get(KEY3), KEY3, VALUE2);

            txn.commit(false);

            // Same data after commit.
            assertEquals((MockStorageMetadata) txn.get(KEY0), KEY0, VALUE0);
            assertEquals((MockStorageMetadata) txn.get(KEY1), KEY1, VALUE1);
            assertEquals((MockStorageMetadata) txn.get(KEY3), KEY3, VALUE2);

        } catch (Exception e) {
            throw e;
        }

        // Step 2: Create a test transaction
        try (MetadataTransaction txn0 = metadataStore.beginTransaction()) {

            // Step 2 A: Make some updates, but don't commit yet.
            txn0.update(new MockStorageMetadata(KEY0, VALUE5));
            assertEquals((MockStorageMetadata) txn0.get(KEY0), KEY0, VALUE5);
            assertEquals((MockStorageMetadata) txn0.get(KEY3), KEY3, VALUE2);

            // Step 3 : Start a parallel transaction
            try (MetadataTransaction txn2 = metadataStore.beginTransaction()) {
                // Step 3 A: Make some updates, but don't commit yet.
                txn2.update(new MockStorageMetadata(KEY0, VALUE6));
                assertEquals((MockStorageMetadata) txn2.get(KEY0), KEY0, VALUE6);
                assertEquals((MockStorageMetadata) txn2.get(KEY3), KEY3, VALUE2);

                // Step 3 B: Commit. It should succeed.
                txn2.commit(false);
                assertEquals((MockStorageMetadata) txn2.get(KEY0), KEY0, VALUE6);
                assertEquals((MockStorageMetadata) txn2.get(KEY3), KEY3, VALUE2);

            } catch (Exception e) {
                throw e;
            }

            // Step 4 : Start a second parallel transaction
            try (MetadataTransaction txn2 = metadataStore.beginTransaction()) {
                // Step 4 : Data committed by earlier transaction should be visible to other new transactions
                assertEquals((MockStorageMetadata) txn2.get(KEY0), KEY0, VALUE6);
                assertEquals((MockStorageMetadata) txn2.get(KEY3), KEY3, VALUE2);
            } catch (Exception e) {
                throw e;
            }

            // A committed transactions should not have any effect on this transaction.
            assertEquals((MockStorageMetadata) txn0.get(KEY0), KEY0, VALUE5);
            assertEquals((MockStorageMetadata) txn0.get(KEY3), KEY3, VALUE2);

            try {
                txn0.commit(false);
                Assert.fail("Transaction should be aborted.");
            } catch (StorageMetadataVersionMismatchException e) {
            }
        }
    }

    @Test
    public void testTransactionFailedForMultipleOperations() throws Exception {

        // Set up the data there are 3 keys
        try (MetadataTransaction txn = metadataStore.beginTransaction()) {
            Assert.assertNull(txn.get(KEY0));
            Assert.assertNull(txn.get(KEY1));
            Assert.assertNull(txn.get(KEY3));
            // Create data
            txn.create(new MockStorageMetadata(KEY0, VALUE0));
            txn.create(new MockStorageMetadata(KEY1, VALUE1));
            txn.create(new MockStorageMetadata(KEY3, VALUE2));

            assertEquals((MockStorageMetadata) txn.get(KEY0), KEY0, VALUE0);
            assertEquals((MockStorageMetadata) txn.get(KEY1), KEY1, VALUE1);
            assertEquals((MockStorageMetadata) txn.get(KEY3), KEY3, VALUE2);

            txn.commit(false);

            // Same data after commit.
            assertEquals((MockStorageMetadata) txn.get(KEY0), KEY0, VALUE0);
            assertEquals((MockStorageMetadata) txn.get(KEY1), KEY1, VALUE1);
            assertEquals((MockStorageMetadata) txn.get(KEY3), KEY3, VALUE2);

        } catch (Exception e) {
            throw e;
        }

        // Start a test transaction
        try (MetadataTransaction txn0 = metadataStore.beginTransaction()) {
            // Update/delete some keys but don't commit .
            // We wan to see that these changes do not affect other transactions
            txn0.update(new MockStorageMetadata(KEY0, VALUE5));
            txn0.delete(KEY1);
            assertEquals((MockStorageMetadata) txn0.get(KEY0), KEY0, VALUE5);
            assertEquals((MockStorageMetadata) txn0.get(KEY3), KEY3, VALUE2);
            Assert.assertNull(txn0.get(KEY1));

            // Another Transaction that must be able modify its view data and commit
            try (MetadataTransaction txn2 = metadataStore.beginTransaction()) {

                txn2.update(new MockStorageMetadata(KEY0, VALUE6));
                txn2.delete(KEY1);
                assertEquals((MockStorageMetadata) txn2.get(KEY0), KEY0, VALUE6);
                assertEquals((MockStorageMetadata) txn2.get(KEY3), KEY3, VALUE2);
                Assert.assertNull(txn2.get(KEY1));

                txn2.commit(false);
                assertEquals((MockStorageMetadata) txn2.get(KEY0), KEY0, VALUE6);
                assertEquals((MockStorageMetadata) txn2.get(KEY3), KEY3, VALUE2);
                Assert.assertNull(txn2.get(KEY1));
            } catch (Exception e) {
                throw e;
            }

            // Yet another Transaction that must be able modify its view data and commit
            try (MetadataTransaction txn2 = metadataStore.beginTransaction()) {

                assertEquals((MockStorageMetadata) txn2.get(KEY0), KEY0, VALUE6);
                assertEquals((MockStorageMetadata) txn2.get(KEY3), KEY3, VALUE2);
                Assert.assertNull(txn2.get(KEY1));
            } catch (Exception e) {
                throw e;
            }

            // A committed transaction should not have any effect on this transaction.
            assertEquals((MockStorageMetadata) txn0.get(KEY0), KEY0, VALUE5);
            assertEquals((MockStorageMetadata) txn0.get(KEY3), KEY3, VALUE2);
            Assert.assertNull(txn0.get(KEY1));

            // However when it tries to commit this should fail.
            try {
                txn0.commit(false);
                Assert.fail("Transaction should be aborted.");
            } catch (StorageMetadataVersionMismatchException e) {

            }
        }
    }

    private void assertEquals(MockStorageMetadata data, String key, String value) {
        Assert.assertEquals("Should get the same key", key, data.getKey());
        Assert.assertEquals("Should get the same data", value, data.getValue());
    }

    private static void assertEquals(MockStorageMetadata expected, MockStorageMetadata actual) {
        Assert.assertEquals(expected.getKey(), actual.getKey());
        Assert.assertEquals(expected.getValue(), actual.getValue());
    }

}
