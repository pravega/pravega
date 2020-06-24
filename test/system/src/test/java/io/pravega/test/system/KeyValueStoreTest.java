package io.pravega.test.system;

import io.pravega.client.ClientConfig;
import io.pravega.client.KeyValueTableFactory;
import io.pravega.client.admin.KeyValueTableInfo;
import io.pravega.client.admin.KeyValueTableManager;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.client.tables.*;
import io.pravega.client.tables.impl.KeyValueTableFactoryImpl;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.*;
import org.junit.runner.RunWith;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.apache.commons.lang.RandomStringUtils.randomAlphanumeric;
import static org.junit.Assert.*;

@Slf4j
@RunWith(SystemTestRunner.class)
public class KeyValueStoreTest extends AbstractSystemTest {

    private static final String SCOPE_NAME = "SampleScope3" + randomAlphanumeric(5);
    private static final String KVT_NAME = "TestKVT1";
    private static final KeyValueTableInfo KVT = new KeyValueTableInfo(SCOPE_NAME,KVT_NAME);
    private static final KeyValueTableConfiguration config = KeyValueTableConfiguration.builder().partitionCount(2).build();
    private static final Serializer<Integer> KEY_SERIALIZER = new IntegerSerializer();
    private static final Serializer<String> VALUE_SERIALIZER = new UTF8StringSerializer();
    private URI controllerURI = null;
    private Controller controller = null;
    private KeyValueTableFactory keyValueTableFactory;
    private KeyValueTable<Integer, String> keyValueTable;
    private ConnectionFactory connectionFactory;
    private Service controllerInstance;

    @Before
    public void setup() throws Exception {
        controllerInstance = Utils.createPravegaControllerService(null);
        List<URI> ctlURIs = controllerInstance.getServiceDetails();
        final List<String> uris = ctlURIs.stream().filter(ISGRPC).map(URI::getAuthority).collect(Collectors.toList());
        controllerURI = URI.create("tcp://" + String.join(",", uris));
        final ClientConfig clientConfig = Utils.buildClientConfig(controllerURI);
        final ScheduledExecutorService controllerExecutor= Executors.newScheduledThreadPool(5);
        this.controller = new ControllerImpl(ControllerImplConfig.builder().clientConfig(clientConfig).build(),controllerExecutor);
        this.connectionFactory = new ConnectionFactoryImpl(clientConfig);
        boolean createdScope = this.controller.createScope(SCOPE_NAME).join();
        assertTrue("The requested Scope must get created", createdScope);
        log.info("Scope {} created successfully", SCOPE_NAME);
        this.keyValueTableFactory = new KeyValueTableFactoryImpl(SCOPE_NAME,this.controller,this.connectionFactory);
        boolean created = this.controller.createKeyValueTable(KVT.getScope(), KVT.getKeyValueTableName(), config).join();
        Assert.assertTrue("The requested KVT must get created",created);
        log.info("KVT {} created successfully", KVT_NAME);
        this.keyValueTable = this.keyValueTableFactory.forKeyValueTable(KVT.getKeyValueTableName(), KEY_SERIALIZER, VALUE_SERIALIZER,
                KeyValueTableClientConfiguration.builder().build());

    }

    @After
    public void tearDown() {
        this.connectionFactory.close();
        this.controller.close();
        this.keyValueTableFactory.close();
        this.keyValueTable.close();
    }

    // Test case-42: Insert Same Key into multiple KeyFamilies
    @Test
    public void test1SameKeyMutipleKF() throws UnsupportedOperationException {
        try {
            String kf1 = "KeyFamily1";
            String kf2 = "KeyFamily2";

            Integer key = 22;
            String value1 = "This value belongs to KeyFamily1";
            String value2 = "This value belongs to KeyFamily2";

            Version version1 = this.keyValueTable.put(kf1, key, value1).join();
            Version version2 = this.keyValueTable.put(kf2, key, value2).join();
            log.info("Version1: {}", version1);
            log.info("Version2: {}", version2);

            TableEntry<Integer, String> tableEntry1 = this.keyValueTable.get(kf1, key).join();
            TableEntry<Integer, String> tableEntry2 = this.keyValueTable.get(kf2, key).join();

            assertEquals("Keys should be same for both entries",
                    tableEntry1.getKey().getKey(), tableEntry2.getKey().getKey());
            assertTrue("Corresponding value for entry1 should be as inserted",
                    value1.equals(tableEntry1.getValue()));
            log.info("Corresponding value '{}' for entry1 is inserted '{}'", value1, tableEntry1.getValue());
            assertTrue("Corresponding value for entry1 should be as inserted",
                    value2.equals(tableEntry2.getValue()));
            log.info("Corresponding value '{}' for entry2 is inserted '{}'", value2, tableEntry2.getValue());
            assertFalse("Corresponding values should be different for both the entries",
                    (tableEntry1.getValue()).equals(tableEntry2.getValue()));
            log.info("Corresponding values are different for both the entries '{}', '{}'",
                    tableEntry1.getValue(), tableEntry2.getValue());

            log.info("Table entry1, key: {} and value: {}", tableEntry1.getKey(), tableEntry1.getValue());
            log.info("Table entry2, key: {} and value: {}", tableEntry2.getKey(), tableEntry2.getValue());
            log.info("Successfully completed test Insert Same Key into multiple KeyFamilies");
        }
        catch (UnsupportedOperationException ex) {
            throw new UnsupportedOperationException();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Test case-43: Conditional insert KVP, if key not exists already
    @Test
    public void test2InsertIfExists() throws UnsupportedOperationException {
        try {
            Integer key = getKeyID();
            String value = getValue();

            Version version1 = this.keyValueTable.putIfAbsent(null, key, value).join();
            assertNotNull("Vesrion1 should not be null", version1);
            log.info("version1: {}", version1);
            TableEntry<Integer, String> tableEntry1 = this.keyValueTable.get(null, key).join();
            assertEquals("Corresponding key for entry1 should be as inserted",
                    key, tableEntry1.getKey().getKey());
            log.info("Get tableEntry1: {}", tableEntry1);
            assertTrue("Corresponding value for entry1 should be as inserted",
                    value.equals(tableEntry1.getValue()));
            log.info("Corresponding value for entry1 is inserted");

            AssertExtensions.assertSuppliedFutureThrows(
                    "putIfAbsent did not throw Exception for already existing key",
                    () -> this.keyValueTable.putIfAbsent(null, key, value),
                    ex -> ex instanceof BadKeyVersionException
            );
            log.info("Found BadKeyVersionException for putIfAbsent insert for already existing key");

            List<Integer> searchKey = new ArrayList<Integer>();
            searchKey.add(key);
            List<TableEntry<Integer, String>> getAllEntry = this.keyValueTable.getAll(null, searchKey).join();
            assertEquals("Only one entry should be present from the fetched list with the given key",
                    1, getAllEntry.size());
            log.info("Only one entry is present from the Fetched list of 'if-exists-test': {}", getAllEntry);
            log.info("Successfully completed test Conditional insert KVP, if key not exists already");
        }
        catch (UnsupportedOperationException ex) {
            throw new UnsupportedOperationException();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Test case-44: Conditional update Key value using its version
    @Test
    public void test3ConditionUpdateKeyValue() throws UnsupportedOperationException {
        try {
            Integer key = getKeyID();
            String value1 = getValue();

            /* Inserting first set of values in the table */
            Version version1 = this.keyValueTable.putIfAbsent(null, key, value1).join();
            assertNotNull("Vesrion1 should not be null", version1);
            log.info("version1: {}", version1);
            TableEntry<Integer, String> tableEntry1 = this.keyValueTable.get(null, key).join();
            log.info("Get tableEntry1: {}", tableEntry1);
            assertEquals("Corresponding key for entry1 should be as inserted",
                    key, tableEntry1.getKey().getKey());
            assertTrue("Corresponding value for entry1 should be as inserted",
                    value1.equals(tableEntry1.getValue()));
            log.info("Corresponding value for tableEntry1 is inserted");

            /* Updating with second set of values in the table */
            String value2 = getValue();
            Version version2 = this.keyValueTable.put(null, key, value2).join();
            assertNotNull("Vesrion2 should not be null", version1);
            log.info("version2: {}", version2);
            TableEntry<Integer, String> tableEntry2 = this.keyValueTable.get(null, key).join();
            log.info("Get tableEntry2: {}", tableEntry2);
            assertEquals("Corresponding key for entry2 should be as inserted",
                    key, tableEntry2.getKey().getKey());
            assertTrue("Corresponding value for entry2 should be as inserted",
                    value2.equals(tableEntry2.getValue()));
            log.info("Corresponding value for tableEntry2 is inserted");
            assertFalse("Corresponding value for entry2 should not remian as entry1",
                    value1.equals(tableEntry2.getValue()));
            log.info("Corresponding value for entry2 is not same as entry1");
            assertNotEquals("Version1 and Version2 should be different in values", version1, version2);
            log.info("Version values are different for both table entries 1 & 2");

            /* Updating third set of values in the table using version of second entry as condition */
            String value3 = getValue();
            Version version3 = this.keyValueTable.replace(null, key, value3, version2).join();
            assertNotNull("Vesrion3 should not be null", version3);
            log.info("version3: {}", version3);
            TableEntry<Integer, String> tableEntry3 = this.keyValueTable.get(null, key).join();
            log.info("Get tableEntry3: {}", tableEntry3);
            assertEquals("Corresponding key for entry3 should be as inserted",
                    key, tableEntry3.getKey().getKey());
            assertTrue("Corresponding value for entry3 should be as inserted",
                    value3.equals(tableEntry3.getValue()));
            log.info("Corresponding value for tableEntry3 is inserted");
            assertFalse("Corresponding value for entry3 should not remian as entry2",
                    value2.equals(tableEntry3.getValue()));
            log.info("Corresponding value for entry3 is not same as entry2");
            assertNotEquals("Version2 and Version3 should be different in values",
                    version2, version3);
            log.info("Version values are different for both table entries 2 & 3");

            /* Trying to update fourth set of values in the table using version of first entry as condition */
            String value4 = getValue();
            AssertExtensions.assertSuppliedFutureThrows(
                    "Should not allow updating the table entry with an older key version",
                    () -> this.keyValueTable.replace(null, key, value4, version1),
                    ex -> ex instanceof BadKeyVersionException
            );
            log.info("Found BadKeyVersionException while updating the table entry with an older key version");

            TableEntry<Integer, String> tableEntry4 = this.keyValueTable.get(null, key).join();
            log.info("Get tableEntry4: {}", tableEntry4);
            assertEquals("Corresponding key for entry4 should be as inserted",
                    key, tableEntry4.getKey().getKey());
            assertFalse("Corresponding value4 for entry4 should not be as inserted",
                    value4.equals(tableEntry4.getValue()));
            assertTrue("Corresponding value3 for entry4 should be as inserted",
                    value3.equals(tableEntry4.getValue()));
            assertFalse("Corresponding value for entry4 should not remian as entry1",
                    value1.equals(tableEntry4.getValue()));
            log.info("Corresponding value3 for tableEntry4 is unchanged");
            log.info("Successfully completed test Conditional update Key value using its version");
        }
        catch (UnsupportedOperationException ex) {
            throw new UnsupportedOperationException();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Test case-45: Conditional delete single Key using its version
    @Test
    public void test4ConditionDeleteKey() throws UnsupportedOperationException {
        try {
            Integer key = getKeyID();
            String value1 = getValue();

            Version version1 = this.keyValueTable.put(null, key, value1).join();
            assertNotNull("Vesrion1 should not be null", version1);
            log.info("version1: " + version1);
            TableEntry<Integer, String> tableEntry1 = this.keyValueTable.get(null, key).join();
            log.info("Get tableEntry1: {}", tableEntry1);
            assertEquals("Corresponding key for entry1 should be as inserted",
                    key, tableEntry1.getKey().getKey());
            assertTrue("Corresponding value for entry1 should be as inserted",
                    value1.equals(tableEntry1.getValue()));
            log.info("Corresponding value1 for tableEntry1 is inserted");

            String value2 = getValue();
            Version version2 = this.keyValueTable.put(null, key, value2).join();
            assertNotNull("Vesrion2 should not be null", version1);
            log.info("version2: " + version2);
            TableEntry<Integer, String> tableEntry2 = this.keyValueTable.get(null, key).join();
            log.info("Get tableEntry2: {}", tableEntry2);
            assertEquals("Corresponding key for entry2 should be as inserted",
                    key, tableEntry2.getKey().getKey());
            assertTrue("Corresponding value for entry2 should be as inserted",
                    value2.equals(tableEntry2.getValue()));
            assertFalse("Corresponding value for entry2 should not remian as entry1",
                    value1.equals(tableEntry2.getValue()));
            log.info("Corresponding value2 for tableEntry2 is inserted");
            assertNotEquals("Version1 and Version2 should be different in values", version1, version2);
            log.info("Version values are different for both table entries 1 & 2");

            AssertExtensions.assertSuppliedFutureThrows(
                    "Table entry with an older key version should not get removed",
                    () -> this.keyValueTable.remove(null, key, version1),
                    ex -> ex instanceof BadKeyVersionException
            );
            log.info("Found BadKeyVersionException while removing the table entry with an older key version");

            assertNull("No error should be obtained during key remove",
                    this.keyValueTable.remove(null, key, version2).join());
            log.info("The requested table entry with specified key & its version is removed");

            assertNull("Null should be returned as the reqested key entry is already removed",
                    this.keyValueTable.get(null, key).join());
            log.info("Null obtained on requsting to fetch a key entry which does not exists");

            AssertExtensions.assertSuppliedFutureThrows(
                    "Error should be obtained on attempt to delete already removed key entry",
                    () -> this.keyValueTable.remove(null, key, version2),
                    ex -> ex instanceof BadKeyVersionException
            );
            log.info("Found BadKeyVersionException on attempt to delete already removed key entry");
            log.info("Successfully completed test Conditional delete single Key using its version");
        }
        catch (UnsupportedOperationException ex) {
            throw new UnsupportedOperationException();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Test case-46: Get Key versions List
    @Test
    public void test5GetKeyVersionList() throws UnsupportedOperationException {
        try {
            String keyFamily = "KeyFamily1";
            List<TableEntry<Integer, String>> entryList = new ArrayList<>();
            Integer[] keyArray = new Integer[10];
            for (int i = 0; i < keyArray.length; i++) {
                keyArray[i] = getKeyID();
            }
            String[] valueArray = new String[10];
            for (int i = 0; i < valueArray.length; i++) {
                valueArray[i] = getValue();
            }

            /* Adding 10 new entries to the Table only if they are not already present */
            for (int i = 0; i < keyArray.length; i++) {
                entryList.add(TableEntry.notExists(keyArray[i], valueArray[i]));
            }
            log.info("Entry list: {}", entryList);
            List<Version> versionList = this.keyValueTable.replaceAll(keyFamily, entryList).join();
            assertNotNull("Version list should not be empty", versionList);
            assertEquals("Version list size should be same as entry list size",
                    versionList.size(),entryList.size());
            log.info("Version list: {}", versionList);

            List<Integer> keyList = new ArrayList<>();
            for (int i = 0; i < keyArray.length; i++) {
                keyList.add(keyArray[i]);
            }
            log.info("Key list: {}", keyList);
            List<TableEntry<Integer, String>> getEntryList = this.keyValueTable.getAll(keyFamily, keyList).join();
            assertNotNull("Get Entry List should not be empty", getEntryList);
            assertEquals("Get Entry List size should be same as get entry list size",
                    getEntryList.size(),entryList.size());
            log.info("Get Entry List: {}", getEntryList);
            for (int i = 0; i < keyArray.length; i++) {
                assertEquals("Corresponding key for getEntryList should be as inserted",
                        keyList.get(i), getEntryList.get(i).getKey().getKey());
                assertEquals("Corresponding key for entryList should be as inserted",
                        entryList.get(i).getKey().getKey(), getEntryList.get(i).getKey().getKey());
            }
            log.info("Corresponding keys for table entries are inserted");

            /* Comparing version values of both obtained versionList and getEntryList */
            for (int i = 0; i < keyArray.length; i++) {
                assertEquals("All version values of both versionList and getEntryList should be equal",
                        versionList.get(i), getEntryList.get(i).getKey().getVersion());
            }
            log.info("Version values for the table entries and obtained versionList are all equal");
            log.info("Successfully completed test Get Key versions List");
        }
        catch (UnsupportedOperationException ex) {
            throw new UnsupportedOperationException();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Test case-47: Conditional insert mutiple entries with KeyFamily
    @Test
    public void test6ConditionInsertMultiKeys() throws UnsupportedOperationException {
        try {
            String keyFamily = "KeyFamily1";
            List<TableEntry<Integer, String>> entryList = new ArrayList<>();
            Integer[] keyArray = new Integer[10];
            for (int i = 0; i < keyArray.length; i++) {
                keyArray[i] = getKeyID();
            }
            String[] valueArray = new String[10];
            for (int i = 0; i < valueArray.length; i++) {
                valueArray[i] = getValue();
            }

            /* Adding 10 new entries to the Table only if they are not already present */
            for (int i = 0; i < keyArray.length; i++) {
                entryList.add(TableEntry.notExists(keyArray[i], valueArray[i]));
            }
            log.info("Entry list: {}", entryList);
            List<Version> versionList = this.keyValueTable.replaceAll(keyFamily, entryList).join();
            assertNotNull("Version list should not be empty", versionList);
            assertEquals("Version list size should be same as entry list size",
                    versionList.size(),entryList.size());
            log.info("Version list: {}", versionList);

            List<Integer> keyList = new ArrayList<>();
            for (int i = 0; i < keyArray.length; i++) {
                keyList.add(keyArray[i]);
            }
            log.info("Key list: {}", keyList);
            List<TableEntry<Integer, String>> getEntryList = this.keyValueTable.getAll(keyFamily, keyList).join();
            assertNotNull("Get Entry List should not be empty", getEntryList);
            assertEquals("Get Entry List size should be same as get entry list size",
                    getEntryList.size(),entryList.size());
            log.info("Get Entry List: {}", getEntryList);
            for (int i = 0; i < keyArray.length; i++) {
                assertEquals("Corresponding key for getEntryList should be as inserted",
                        keyList.get(i), getEntryList.get(i).getKey().getKey());
                assertEquals("Corresponding key for entryList should be as inserted",
                        entryList.get(i).getKey().getKey(), getEntryList.get(i).getKey().getKey());
            }
            log.info("Corresponding keys for table entries are inserted");

            /* Comparing each & every entry values of both entrylist and getEntryList */
            for (int i = 0; i < keyArray.length; i++) {
             assertTrue("All string values of both entryList and getEntryList should be equal",
                     (entryList.get(i).getValue()).equals(getEntryList.get(i).getValue()));
            }
            log.info("All string values of both entryList and getEntryList are equal");
            log.info("Successfully completed test Conditional insert mutiple entries with KeyFamily");
        }
        catch (UnsupportedOperationException ex) {
            throw new UnsupportedOperationException();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Test case-48: Conditional update multiple Key value with KF using its version
    @Test
    public void test7ConditionUpdateMultiKeys() throws UnsupportedOperationException {
        try {
            String keyFamily = "KeyFamily1";
            List<TableEntry<Integer, String>> entryList1 = new ArrayList<>();
            Integer[] keyArray = new Integer[10];
            for (int i = 0; i < keyArray.length; i++) {
                keyArray[i] = getKeyID();
            }
            String[] valueArray1 = new String[10];
            for (int i = 0; i < valueArray1.length; i++) {
                valueArray1[i] = getValue();
            }

            /* Adding 10 new entries to the Table only if they are not already present */
            for (int i = 0; i < keyArray.length; i++) {
                entryList1.add(TableEntry.notExists(keyArray[i], valueArray1[i]));
            }
            log.info("Entry list1: {}", entryList1);
            List<Version> versionList1 = this.keyValueTable.replaceAll(keyFamily, entryList1).join();
            assertNotNull("Version list1 should not be empty", versionList1);
            assertEquals("Version list1 size should be same as entry list1 size",
                    versionList1.size(), entryList1.size());
            log.info("Version list1: {}", versionList1);

            List<Integer> keyList = new ArrayList<>();
            for (int i = 0; i < keyArray.length; i++) {
                keyList.add(keyArray[i]);
            }
            log.info("Key list: {}", keyList);
            List<TableEntry<Integer, String>> getEntryList1 = this.keyValueTable.getAll(keyFamily, keyList).join();
            assertNotNull("Get Entry List1 should not be empty", getEntryList1);
            assertEquals("Get Entry List1 size should be same as get entry list1 size",
                    getEntryList1.size(), entryList1.size());
            log.info("Get Entry List1: {}", getEntryList1);
            for (int i = 0; i < keyArray.length; i++) {
                assertEquals("Corresponding key for getEntryList should be as inserted",
                        keyList.get(i), getEntryList1.get(i).getKey().getKey());
                assertEquals("Corresponding key for entryList should be as inserted",
                        entryList1.get(i).getKey().getKey(), getEntryList1.get(i).getKey().getKey());
            }
            log.info("Corresponding keys for table entries are inserted");

            /* Comparing each & every entry values of both entrylist and getEntryList */
            for (int i = 0; i < keyArray.length; i++) {
                assertTrue("All string values of both entryList1 and getEntryList1 should be equal",
                        (entryList1.get(i).getValue()).equals(getEntryList1.get(i).getValue()));
            }
            log.info("All string values of both entryList1 and getEntryList1 are equal");

            /* Preparing entryList2 with versioned table entry as obtained from versionList1 */
            List<TableEntry<Integer, String>> entryList2 = new ArrayList<>();
            String[] valueArray2 = new String[10];
            for (int i = 0; i < valueArray1.length; i++) {
                valueArray2[i] = getValue();
            }
            for (int i = 0; i < keyArray.length; i++) {
                entryList2.add(TableEntry.versioned(keyArray[i], versionList1.get(i), valueArray2[i]));
            }
            log.info("Entry list2: {}", entryList2);

            /* Conditionally updating the values of all the Keys with the specified key version and KeyFamily */
            List<Version> versionList2 = this.keyValueTable.replaceAll(keyFamily, entryList2).join();
            assertNotNull("Version list2 should not be empty", versionList2);
            assertEquals("Version list2 size should be same as entry list2 size",
                    versionList2.size(), entryList2.size());
            log.info("Version list2: {}", versionList2);

            log.info("Key list: {}", keyList);
            List<TableEntry<Integer, String>> getEntryList2 = this.keyValueTable.getAll(keyFamily, keyList).join();
            assertNotNull("Get Entry List2 should not be empty", getEntryList2);
            assertEquals("Get Entry List2 size should be same as get entry list2 size",
                    getEntryList2.size(), entryList2.size());
            log.info("Get Entry List2: {}", getEntryList2);
            for (int i = 0; i < keyArray.length; i++) {
                assertEquals("Corresponding key for getEntryList should be as inserted",
                        keyList.get(i), getEntryList2.get(i).getKey().getKey());
                assertEquals("Corresponding key for entryList should be as inserted",
                        entryList2.get(i).getKey().getKey(), getEntryList2.get(i).getKey().getKey());
            }
            log.info("Corresponding keys for table entries are inserted");

            /* Comparing each & every entry values of both entrylist and getEntryList */
            for (int i = 0; i < keyArray.length; i++) {
                assertTrue("All string values of both entryList2 and getEntryList2 should be equal",
                        (entryList2.get(i).getValue()).equals(getEntryList2.get(i).getValue()));
            }
            log.info("All string values of both entryList2 and getEntryList2 are equal");
            /* Comparing each & every version values of both versionList1 and versionList2 */
            for (int i = 0; i < keyArray.length; i++) {
                assertFalse("All version values of both getEntryList1 and getEntryList2 should not be equal",
                        (getEntryList1.get(i).getKey().getVersion()).equals(getEntryList2.get(i).getKey().getVersion()));
                assertNotEquals("All version values of both versionList1 and versionList2 should not be equal",
                        versionList1.get(i),versionList2.get(i));
            }
            log.info("Version values for the updated table entries are also updated with new values");
            log.info("Successfully completed test Conditional update multiple Key values with KF using its version");

        }
        catch (UnsupportedOperationException ex) {
            throw new UnsupportedOperationException();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Test case-49: Conditional delete Key with KeyFamily using its version
    @Test
    public void test8ConditionDeleteMultikeys() throws UnsupportedOperationException {
        try {
            String keyFamily = "KeyFamily1";
            List<TableEntry<Integer, String>> entryList1 = new ArrayList<>();
            Integer[] keyArray = new Integer[10];
            for (int i = 0; i < keyArray.length; i++) {
                keyArray[i] = getKeyID();
            }
            String[] valueArray1 = new String[10];
            for (int i = 0; i < valueArray1.length; i++) {
                valueArray1[i] = getValue();
            }

            /* Adding 10 new entries to the Table only if they are not already present */
            for (int i = 0; i < keyArray.length; i++) {
                entryList1.add(TableEntry.notExists(keyArray[i], valueArray1[i]));
            }
            log.info("Entry list1: {}", entryList1);
            List<Version> versionList1 = this.keyValueTable.replaceAll(keyFamily, entryList1).join();
            assertNotNull("Version list1 should not be empty", versionList1);
            assertEquals("Version list1 size should be same as entry list1 size",
                    versionList1.size(), entryList1.size());
            log.info("Version list1: {}", versionList1);

            List<Integer> keyList = new ArrayList<>();
            for (int i = 0; i < keyArray.length; i++) {
                keyList.add(keyArray[i]);
            }
            log.info("Key list: {}", keyList);
            List<TableEntry<Integer, String>> getEntryList1 = this.keyValueTable.getAll(keyFamily, keyList).join();
            assertNotNull("Get Entry List1 should not be empty", getEntryList1);
            assertEquals("Get Entry List1 size should be same as get entry list1 size",
                    getEntryList1.size(), entryList1.size());
            log.info("Get Entry List1: {}", getEntryList1);
            for (int i = 0; i < keyArray.length; i++) {
                assertEquals("Corresponding key for getEntryList should be as inserted",
                        keyList.get(i), getEntryList1.get(i).getKey().getKey());
                assertEquals("Corresponding key for entryList should be as inserted",
                        entryList1.get(i).getKey().getKey(), getEntryList1.get(i).getKey().getKey());
            }
            log.info("Corresponding keys for table entries are inserted");

            /* Comparing each & every entry values of both entrylist and getEntryList */
            for (int i = 0; i < keyArray.length; i++) {
                assertTrue("All string values of both entryList1 and getEntryList1 should be equal",
                        (entryList1.get(i).getValue()).equals(getEntryList1.get(i).getValue()));

            }
            log.info("All string values of both entryList1 and getEntryList1 are equal");

            List<TableKey<Integer>> tableKeyList = new ArrayList<>();
            for (int i = 0; i < getEntryList1.size(); i++) {
                tableKeyList.add(getEntryList1.get(i).getKey());
            }
            log.info("Table Key List: {}", tableKeyList);

            assertNull("No error should be obtained during key remove",
                    this.keyValueTable.removeAll(keyFamily, tableKeyList).join());
            log.info("All the requested table entries with specified key & its version are removed");

            List<TableEntry<Integer,String>> getEntryList2 = this.keyValueTable.getAll(keyFamily, keyList).join();
            log.info("Get entry list2: {}", getEntryList2);
            for (int i = 0; i < getEntryList2.size(); i++) {
                assertNull("All table entries fetched for deleted/not available keys should be null",
                        getEntryList2.get(i));
            }
            log.info("Null obtained on requesting to fetch all key entries which does not exists");

            AssertExtensions.assertSuppliedFutureThrows(
                    "",
                    () -> this.keyValueTable.removeAll(keyFamily, tableKeyList),
                    ex -> ex instanceof NoSuchKeyException
            );
            log.info("Found NoSuchKeyException on attempt to delete already removed key entries");
            log.info("Successfully completed test Conditional delete Key with KeyFamily using its version");

        }
        catch (UnsupportedOperationException ex) {
            throw new UnsupportedOperationException();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Test case-50: Delete KVT and check KVPs are not accessible
    @Test
    @Ignore
    public void test9DeleteKVTRetriveKVP() throws UnsupportedOperationException {
        try {
            String NEW_KVT_NAME = "TestKVT2";
            @Cleanup
            KeyValueTableManager manager = KeyValueTableManager.create(controllerURI);
            KeyValueTableInfo NEW_KVT = new KeyValueTableInfo(SCOPE_NAME,NEW_KVT_NAME);
            boolean created = manager.createKeyValueTable(SCOPE_NAME, NEW_KVT_NAME, config);
            Assert.assertTrue("The requested KVT must get created", created);
            KeyValueTable<Integer, String> newKeyValueTable = this.keyValueTableFactory.forKeyValueTable(NEW_KVT.getKeyValueTableName(),
                    KEY_SERIALIZER, VALUE_SERIALIZER, KeyValueTableClientConfiguration.builder().build());

            String keyFamily = "KeyFamily1";
            LinkedHashMap<Integer, String> entryMap1 = new LinkedHashMap<>();
            Integer[] keyArray = new Integer[10];
            for (int i = 0; i < keyArray.length; i++) {
                keyArray[i] = getKeyID();
            }
            String[] valueArray1 = new String[10];
            for (int i = 0; i < valueArray1.length; i++) {
                valueArray1[i] = getValue();
            }

            /* Adding 10 new entries to the Table only if they are not already present */
            for (int i = 0; i < keyArray.length; i++) {
                entryMap1.put(keyArray[i], valueArray1[i]);
            }

            List<Version> versionList1 = newKeyValueTable.putAll(keyFamily, entryMap1.entrySet()).join();
            log.info("Version list1: {}", versionList1);
            assertNotNull("Version list1 should not be empty", versionList1);
            assertEquals("Version list1 size should be same as entry list1 size",
                    versionList1.size(), entryMap1.size());

            List<Integer> keyList = new ArrayList<>();
            for (int i = 0; i < keyArray.length; i++) {
                keyList.add(keyArray[i]);
            }
            log.info("Key list: {}", keyList);
            List<TableEntry<Integer, String>> getEntryList1 = newKeyValueTable.getAll(keyFamily, keyList).join();
            log.info("Get Entry List1: {}", getEntryList1);
            assertNotNull("Get Entry List1 should not be empty", getEntryList1);
            assertEquals("Get Entry List1 size should be same as get entry list1 size",
                    getEntryList1.size(), entryMap1.size());
            List<Integer> entryKeyList = new ArrayList<>();
            for (Integer key: entryMap1.keySet()) {
                entryKeyList.add(key);
            }
            log.info("entryKeyList: {}", entryKeyList);
            for (int i = 0; i < keyArray.length; i++) {
                assertEquals("Corresponding key for getEntryList should be as inserted",
                        keyList.get(i), getEntryList1.get(i).getKey().getKey());
                assertEquals("Corresponding key for entryList should be as inserted",
                        entryKeyList.get(i), getEntryList1.get(i).getKey().getKey());
            }
            log.info("Corresponding keys for table entries are inserted");

            /* Comparing each & every entry values of both entrylist and getEntryList */
            for (int i = 0; i < keyArray.length; i++) {
                assertTrue("All string values of both entryList1 and getEntryList1 should be equal",
                        (entryMap1.get(entryKeyList.get(i))).equals(getEntryList1.get(i).getValue()));
            }
            log.info("All string values of both entryMap1 and getEntryList1 are equal");

         /*  boolean deleted = manager.deleteKeyValueTable(SCOPE_NAME, NEW_KVT_NAME);
            assertTrue("The requested KVT must get deleted", deleted);  */

            boolean deleted = this.controller.deleteKeyValueTable(SCOPE_NAME, NEW_KVT_NAME).join();
            log.info("KVT '{}' got deleted", NEW_KVT_NAME);

            /* Any operation performed over the above deleted Table must fail there upon */
            List<TableEntry<Integer, String>> getEntryList2 = newKeyValueTable.getAll(keyFamily, keyList).join();
            for (int i=0; i<getEntryList2.size(); i++) {
                assertNull("All entries of getEntryList2 should be null", getEntryList2.get(i));
            }
            log.info("All entries of getEntryList2 are set to null");
            log.info("Successfully completed test Delete KVT and check KVPs are not accessible");
        }
        catch (UnsupportedOperationException ex) {
            throw new UnsupportedOperationException();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Integer getKeyID() {
        Integer keyId = ThreadLocalRandom.current().nextInt(1000,99999);
        return keyId;
    }

    public String getValue() {
        String value = "Hello World";
        log.debug("Initial value length: {}", value.getBytes().length);
        while (value.getBytes().length <= 10302) {
           value+= ThreadLocalRandom.current().nextInt(1000, 9999);
        }
        log.debug("Final value length: {}", value.getBytes().length);
        return value;
    }

    private static class IntegerSerializer implements Serializer<Integer> {
        @Override
        public ByteBuffer serialize(Integer value) {
            return ByteBuffer.allocate(Integer.BYTES).putInt(0, value);
        }

        @Override
        public Integer deserialize(ByteBuffer serializedValue) {
            return serializedValue.getInt();
        }
    }
}
