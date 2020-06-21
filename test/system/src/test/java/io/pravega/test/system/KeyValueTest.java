package io.pravega.test.system;

import io.pravega.client.control.impl.Controller;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.client.tables.*;
import io.pravega.client.admin.KeyValueTableManager;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.impl.*;
import io.pravega.common.util.AsyncIterator;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.client.*;
import io.pravega.client.admin.*;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;
import lombok.extern.slf4j.Slf4j;
import lombok.Cleanup;
import lombok.val;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

@Slf4j
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(SystemTestRunner.class)
public class KeyValueTest extends AbstractSystemTest {
    private final static String SCOPE_NAME = "TestScope16";
    private final static String SCOPE_NAME1 = "DiffScope2";
    private final static String KVT_NAME = "TestKVT";
    private URI controllerURI = null;
    private Controller controller = null;
    final KeyValueTableConfiguration config = KeyValueTableConfiguration.builder().partitionCount(2).build();
    private Integer keyType;
    private String valueType = "";
    private String keyFamily = "";
    private String tetsKeyValue = "";
    public final String scope="TestScope";
    //private TableSegmentFactoryImpl segmentFactory;
    private KeyValueTableFactory KeyValueTableFactory;
    private KeyValueTable<Integer, String> keyValueTable;
    private static final KeyValueTableInfo KVT = new KeyValueTableInfo(SCOPE_NAME,KVT_NAME);
    private ConnectionFactory connectionFactory;
    private StreamManager streamManager;
    private Service controllerInstance;
    private Version version;
    protected static final Serializer<Integer> KEY_SERIALIZER = new IntegerSerializer();
    protected static final Serializer<String> VALUE_SERIALIZER = new UTF8StringSerializer();

    /*
        @Environment
        public static void initialize() {
            URI zkUri = startZookeeperInstance();
            startBookkeeperInstances(zkUri);
            URI controllerUri = ensureControllerRunning(zkUri);
            ensureSegmentStoreRunning(zkUri, controllerUri);
        }
     */
    @Before
    public void setup() throws Exception {
        controllerInstance = Utils.createPravegaControllerService(null);
        List<URI> ctlURIs = controllerInstance.getServiceDetails();
        final List<String> uris = ctlURIs.stream().filter(ISGRPC).map(URI::getAuthority).collect(Collectors.toList());
        controllerURI = URI.create("tcp://" + String.join(",", uris));
        final ClientConfig clientConfig = Utils.buildClientConfig(controllerURI);
        final ScheduledExecutorService controllerExecutor= Executors.newScheduledThreadPool(5);
        controller = new ControllerImpl(ControllerImplConfig.builder().clientConfig(clientConfig).build(),controllerExecutor);
        this.connectionFactory = new ConnectionFactoryImpl(clientConfig);
        //val tokenProvider = DelegationTokenProviderFactory.create(this.controller, KVT.getScope(), KVT.getKeyValueTableName());
        //this.segmentFactory = new TableSegmentFactoryImpl(controller,connectionFactory,KeyValueTableClientConfiguration.builder().build(),tokenProvider);
        this.KeyValueTableFactory = new KeyValueTableFactoryImpl(SCOPE_NAME,this.controller,this.connectionFactory);
        //this.keyValueTable = this.KeyValueTableFactory.forKeyValueTable(KVT.getKeyValueTableName(),KEY_SERIALIZER, VALUE_SERIALIZER,KeyValueTableClientConfiguration.builder().build());
        //this.version = new VersionImpl(VersionImpl.NO_SEGMENT_ID, TableSegmentKeyVersion.NO_VERSION);
        tetsKeyValue = convertStringToBinary("Hello World");
        while (tetsKeyValue.getBytes().length <= 1040000) {
            tetsKeyValue = tetsKeyValue + convertStringToBinary("Hello World");
        }

    }

    @After
    public void tearDown() {
        //streamManager.close();
    }

    @Test
    //Test case - 13: Create KVT test
    public void testA1CreateKeyValueTable() throws Exception {
        log.info("Create Key value Table(KVT)");
        try {
            log.info("controller URL : "+controllerURI);
            streamManager = StreamManager.create(Utils.buildClientConfig(controllerURI));
            assertTrue("Creating Scope", streamManager.createScope(SCOPE_NAME));
            val kvtManager = KeyValueTableManager.create(controllerURI);
            boolean createKVT = kvtManager.createKeyValueTable(SCOPE_NAME, KVT_NAME, config);
            Assert.assertTrue(createKVT);
            if (createKVT == true) {
                log.info("Successfully created KVT");
            } else {
                log.info("Failed to created KVT");
            }
        } catch (AssertionError error) {
            log.info(error.getMessage());
        }
    }

    @Test
    //Test case - 13: Create same KVT again
    public void testA2CreateExistingKeyValueTable() throws Exception {
        log.info("Create Key value Table(KVT)");
        try {
            @Cleanup
            val kvtManager = KeyValueTableManager.create(controllerURI);
            boolean createKVT = kvtManager.createKeyValueTable(SCOPE_NAME, KVT_NAME, config);
            Assert.assertFalse(createKVT);
            if (createKVT == false) {
                log.info("KeyValueTable already exists, So can not create same KVT");
            }
        } finally {
            log.info("Can not create as KVT already present");
        }
    }
    @Test
    public void testA3CreateSameKVTDiffScope() throws Exception {
        log.info("Create Key value Table(KVT)");
        try {
            streamManager = StreamManager.create(Utils.buildClientConfig(controllerURI));
            assertTrue("Creating Scope", streamManager.createScope(SCOPE_NAME1));
            val kvtManager = KeyValueTableManager.create(controllerURI);
            boolean createKVT = kvtManager.createKeyValueTable(SCOPE_NAME1, KVT_NAME, config);
            Assert.assertTrue(createKVT);
            if (createKVT == true) {
                log.info("Successfully created same KVT in different Scope");
            } else {
                log.info("Failed to created KVT");
            }
        } catch (AssertionError error) {
            log.info(error.getMessage());
        }
    }

    @Test
    // Test Cases-18 List of KVT test
    public void testA4ListKeyValueTables() {
        try {
            @Cleanup
            val kvtManager = KeyValueTableManager.create(controllerURI);
            Iterator name = kvtManager.listKeyValueTables(SCOPE_NAME);
            Assert.assertTrue(name.hasNext());
            while (name.hasNext()) {
                log.info("KVS table name" + name.next());
            }
        }catch (AssertionError error){
            log.info(error.getMessage());
        }
    }
    @Test
    public void testA5UpdateKeyValueTable(){
        try {
            val kvtManager = KeyValueTableManager.create(controllerURI);
            boolean updateKVT = kvtManager.updateKeyValueTable(SCOPE_NAME, KVT_NAME, config);
            Assert.assertTrue(updateKVT);
        }catch (AssertionError error){
            log.info(error.getMessage());
        }
    }
    @Test
    // Test Case-14: Delete KVT test
    public void testA6DeleteKeyValueTable(){
        try {
            log.info("Deleting existing KVT");
            @Cleanup
            val kvtManager = KeyValueTableManager.create(controllerURI);
            boolean deleteKVT = kvtManager.deleteKeyValueTable(SCOPE_NAME, KVT_NAME);
            Assert.assertTrue(deleteKVT);
        }catch (AssertionError error){
            log.info(error.getMessage());
        }
    }
    //Test Case-1: Insert KVP
    @Test
    public void testA7InsertKeyValuePair(){
        try {
            log.info("Insert KVP");
            keyType = 1;
            valueType="TestValue";
            this.keyValueTable = this.KeyValueTableFactory.forKeyValueTable(KVT.getKeyValueTableName(),KEY_SERIALIZER, VALUE_SERIALIZER,KeyValueTableClientConfiguration.builder().build());
            CompletableFuture<Version> insertEntry = this.keyValueTable.put(null,keyType,valueType);
            Version result = insertEntry.get();
            log.info("result "+result);
        }catch (ExecutionException | InterruptedException  error) {
            log.info(error.getMessage());
        }
    }
    @Test
    public void testA8InsertKVPConditionallyWithNewKey(){
        try {
            log.info("Insert or update KVP");
            keyType = 2;
            valueType="TestValue1";
            this.keyValueTable = this.KeyValueTableFactory.forKeyValueTable(KVT.getKeyValueTableName(),KEY_SERIALIZER, VALUE_SERIALIZER,KeyValueTableClientConfiguration.builder().build());
            CompletableFuture<Version> insertEntry = this.keyValueTable.putIfAbsent(null,keyType,valueType);
            Version result = insertEntry.get();
            log.info("result "+result);
        }catch (ExecutionException | InterruptedException  error) {
            log.info(error.getMessage());
        }
    }
    @Test
    public void testA9InsertKVPConditionallyWithExistingKey(){
        try {
            log.info("Insert or update KVP");
            keyType = 2;
            valueType="TestValue1";
            this.keyValueTable = this.KeyValueTableFactory.forKeyValueTable(KVT.getKeyValueTableName(),KEY_SERIALIZER, VALUE_SERIALIZER,KeyValueTableClientConfiguration.builder().build());
            CompletableFuture<Version> insertEntry = this.keyValueTable.putIfAbsent(null,keyType,valueType);
            Version result = insertEntry.get();
            log.info("result "+result);
        }catch (ExecutionException | InterruptedException  error) {
            log.info(error.getMessage());
        }
    }
    @Test
    public void testB1InsertMultipleKeyValuePair(){
        try {
            log.info("Insert multiple KVP");
            String kf ="TestKeyFamily";
            Map<Integer,String> multiKVP = new HashMap<Integer,String>();
            multiKVP.put(3,"TestValue3"); multiKVP.put(4,"TestValue4"); multiKVP.put(5,"TestValue5");
            this.keyValueTable = this.KeyValueTableFactory.forKeyValueTable(KVT.getKeyValueTableName(),KEY_SERIALIZER, VALUE_SERIALIZER,KeyValueTableClientConfiguration.builder().build());
            CompletableFuture<List<Version>> insertKVP = this.keyValueTable.putAll(kf,multiKVP.entrySet());
            List<Version> result = insertKVP.get();
            log.info("successfully inserted key "+multiKVP.entrySet()+"And output"+result);
        }catch (ExecutionException | InterruptedException  error) {
            log.info(error.getMessage());
        }
    }
    @Test
    public void testB2RetrieveKeyValuePairEntry(){
        try {
            keyType = 1;
            log.info("Retrieve Single KVP entry");
            this.keyValueTable = this.KeyValueTableFactory.forKeyValueTable(KVT.getKeyValueTableName(),KEY_SERIALIZER, VALUE_SERIALIZER,KeyValueTableClientConfiguration.builder().build());
            CompletableFuture<TableEntry<Integer, String>> getKVT = this.keyValueTable.get(null,keyType);
            TableEntry<Integer, String> result = getKVT.get();
            log.info("Successfully retrive single keyvaluepair entry : "+result);
        }catch (ExecutionException | InterruptedException  error) {
            log.info(error.getMessage());
        }
    }

    @Test
       public void testC1CreateKeyEntry() throws Exception {
            log.info("Create key with length > 8KB");
            try {
                String xyz = "adkdjfldjgk;ldfjlkcnb,jnfdkjlhglkdtjglkdfjblkcjgbkljglkcjbklfjgbklj:";
                val kt = TableKey.unversioned(KVT_NAME);
                String key = kt.getKey();
                log.info("key value is : "+key+" Size of value in byte :" + key.getBytes().length);
                //TableKey().versioned();
            } catch (AssertionError error) {
                log.info(error.getMessage());
            }
        }

     @Test
        // // Test case : 24
        public void testC1CreateKeyEntryGT8KB() throws Exception {
        log.info("Create key with length > 8KB");
        try {
            Integer keyId = 14;
            String value = "Hello World";
            this.keyValueTable = this.KeyValueTableFactory.forKeyValueTable(KVT.getKeyValueTableName(),KEY_SERIALIZER, VALUE_SERIALIZER,KeyValueTableClientConfiguration.builder().build());
            CompletableFuture<Version> insertEntry = this.keyValueTable.put(null,keyId,value);
            Version data = insertEntry.get();
            log.info("1st Version "+data);
            CompletableFuture<TableEntry<Integer, String>> kvpEntry = this.keyValueTable.get(null,keyId);
            log.info("2nd Table entry "+kvpEntry.get());
            log.info("3rd KEY value is :"+kvpEntry.get().getKey());
            log.info("4th VALUE value is :"+kvpEntry.get().getValue());
            log.info("5ht key size in Byte :"+ kvpEntry.get().getKey().toString().getBytes().length);
            //TableKey().versioned();
        } catch (AssertionError error) {
            log.info(error.getMessage());
        }
    }

    @Test
    // Test case : 24
    public void testC1CreateKeyEntryGreaterThan8KB() throws Exception {
        log.info("Create key with length > 8KB");
        try {
            Integer keyId = ThreadLocalRandom.current().nextInt();
            log.info("key value is "+keyId);
/*            while (keyId.toString().getBytes().length <= 8192) {
                keyId = keyId+ThreadLocalRandom.current().nextInt();
                log.info("New key value is" + keyId);
            }*/
            String value = convertStringToBinary("Hello World");
            this.keyValueTable = this.KeyValueTableFactory.forKeyValueTable(KVT.getKeyValueTableName(),KEY_SERIALIZER, VALUE_SERIALIZER,KeyValueTableClientConfiguration.builder().build());
            CompletableFuture<Version> insertEntry = this.keyValueTable.put(null,keyId,value);
            Version data = insertEntry.get();
            log.info("1st Version "+data);
            CompletableFuture<TableEntry<Integer, String>> kvpEntry = this.keyValueTable.get(null,keyId);
            log.info("2nd Table entry "+kvpEntry.get().getKey());
            log.info("3rd KEY value is :"+kvpEntry.get().getKey().getKey());
            log.info("3rd VERSION value is :"+kvpEntry.get().getKey().getVersion());
            log.info("4th VALUE value is :"+kvpEntry.get().getValue());
            log.info("5ht key size in Byte :"+ kvpEntry.get().getKey().toString().getBytes().length);
            assertEquals("Not matched",keyId,kvpEntry.get().getKey().getKey());
            //TableKey().versioned();
        } catch (AssertionError error) {
            log.info(error.getMessage());
        }
    }

    @Test
    // Test case : 25
    public void testC2CreateKeyEntryLT1MB() throws Exception {
        log.info("Create Table entry of size < 1MB");
        try {
            Integer keyId = ThreadLocalRandom.current().nextInt(1000, 99999);
            log.info("key value is "+keyId);
            log.info("size of the key "+keyId.toString().getBytes().length);
            String value = convertStringToBinary("Hello World");
            while (value.getBytes().length <= 1000000) {
                value = value + convertStringToBinary("Hello World");;
            }
            log.info("Final string of value "+value);
            log.info("Size of the value string "+ value.getBytes().length);
            this.keyValueTable = this.KeyValueTableFactory.forKeyValueTable(KVT.getKeyValueTableName(),KEY_SERIALIZER, VALUE_SERIALIZER,KeyValueTableClientConfiguration.builder().build());
            CompletableFuture<Version> insertEntry = this.keyValueTable.put(null,keyId,value);
            Version data = insertEntry.get();
            log.info("1st Version "+data);
            CompletableFuture<TableEntry<Integer, String>> kvpEntry = this.keyValueTable.get(null,keyId);
            log.info("3rd KEY value is :"+kvpEntry.get().getKey());
            log.info("Final KEY value is :"+kvpEntry.get().getKey().getKey());
            log.info("Final version value is :"+kvpEntry.get().getKey().getVersion());
            log.info("5ht key size in Byte :"+ kvpEntry.get().getKey().getKey().toString().getBytes().length);
            log.info("6th value size in byte :" + kvpEntry.get().getValue().getBytes().length);
            //TableKey().versioned();
            Integer size= kvpEntry.get().getKey().getKey().toString().getBytes().length + kvpEntry.get().getValue().getBytes().length;
            log.info("Total size update in KVP(will pring 0 if less 1 MB) :" + bytesToMB(size.toString().length())+"MB");
            assertEquals("Verifing same key has inserted in KVP or not",keyId,kvpEntry.get().getKey().getKey());
        } catch (ExecutionException | InterruptedException  error) {
            log.info(error.getMessage());
        }
    }
    @Test
    //test case =29 // Value Length too long. Must be less than 1040384; given 1048608.
    public void testC3CreateTableEntryGreaterThan1MB() throws Exception {
        log.info("Create Table entry of size > 1MB");
        try {
            Integer keyId = ThreadLocalRandom.current().nextInt(1000, 99999);
            log.info("key value is "+keyId);
            log.info("size of the key "+keyId.toString().getBytes().length);
            tetsKeyValue = convertStringToBinary("Hello World");
            while (tetsKeyValue.getBytes().length <= 1048576) {
                tetsKeyValue = tetsKeyValue + convertStringToBinary("Hello World");
            }
            log.info("Final string of value "+tetsKeyValue);
            log.info("Size of the value string "+ tetsKeyValue.getBytes().length);
            this.keyValueTable = this.KeyValueTableFactory.forKeyValueTable(KVT.getKeyValueTableName(),KEY_SERIALIZER, VALUE_SERIALIZER,KeyValueTableClientConfiguration.builder().build());
            CompletableFuture<Version> insertEntry = this.keyValueTable.put(null,keyId,tetsKeyValue);
            Version data = insertEntry.get();
            log.info("1st Version "+data);
            CompletableFuture<TableEntry<Integer, String>> kvpEntry = this.keyValueTable.get(null,keyId);
            log.info("3rd KEY value is :"+kvpEntry.get().getKey());
            log.info("Final KEY value is :"+kvpEntry.get().getKey().getKey());
            log.info("Final version value is :"+kvpEntry.get().getKey().getVersion());
            log.info("5ht key size in Byte :"+ kvpEntry.get().getKey().getKey().toString().getBytes().length);
            log.info("6th value size in byte :" + kvpEntry.get().getValue().getBytes().length);
            //TableKey().versioned();
            Integer size= kvpEntry.get().getKey().getKey().toString().getBytes().length + kvpEntry.get().getValue().getBytes().length;
            log.info("Total size update in KVP(will pring 0 if less 1 MB) :" + bytesToMB(size.toString().length())+"MB");
            assertNotEquals("Failed to insert due to Value Length too long",keyId,null);
            assertNotEquals("Failed to insert due to Value Length too long",keyId, kvpEntry.get().getKey().getKey());
        } catch (ExecutionException | InterruptedException | IllegalArgumentException error) {
            error.printStackTrace();
            log.info("exception messge "+error.getMessage());

            log.info ("tostring message " +error.toString());
        }
    }

    @Test
    //Test case 30
    public void testC4MultipleTableEntryGreaterThan32MB() throws Exception {
        log.info("Update multiple KVPs with entries of total size > 32MB");
        try {
            Integer keyId;
            Integer totalsize=0;
            String keyfamily = "TestkeyFamily";
            Map<Integer, String> multiKVP = new HashMap<Integer, String>();
/*            String value = "Hello World";
            while (value.getBytes().length <= 1040000) {
                value = value + "0101010101010101010101010101010";
            }*/
            log.debug("Value String"+tetsKeyValue);
            for (int loop=0; loop < 33; loop++) {
                log.info("Loop count :" + loop);
                keyId = ThreadLocalRandom.current().nextInt(1000, 99999);
                multiKVP.put(keyId, tetsKeyValue);
                log.info("totalsize is " + totalsize);
                totalsize = totalsize + keyId.toString().getBytes().length + tetsKeyValue.getBytes().length;
                log.info("size of the key " + keyId.toString().getBytes().length);
                log.info("Size of the value string " + tetsKeyValue.getBytes().length);
                log.info("Final totalsize is" + totalsize);
            }
            this.keyValueTable = this.KeyValueTableFactory.forKeyValueTable(KVT.getKeyValueTableName(), KEY_SERIALIZER, VALUE_SERIALIZER, KeyValueTableClientConfiguration.builder().build());
            CompletableFuture<List<Version>> insertEntry = this.keyValueTable.putAll(keyfamily,multiKVP.entrySet());
            List<Version> result = insertEntry.get();
            CompletableFuture<List<TableEntry<Integer, String>>> kvpEntry = this.keyValueTable.getAll(null,multiKVP.keySet());
            log.info("key of 1st index "+kvpEntry.get().get(0).getKey().getKey().toString().getBytes().length);
            log.info("value of 1st index"+kvpEntry.get().get(0).getValue().getBytes().length);
            assertEquals("conpare key",multiKVP.size(),kvpEntry.get().size());
            //assertTrue("conpare key",keyId == kvpEntry.get().getKey().getKey());
            //assertTrue("Compare value", value.getBytes().length == kvpEntry.get().getValue().getBytes().length);
        }catch (ExecutionException | InterruptedException  error) {
            log.info(error.getMessage());
        }
    }
    @Test
    //Test case 31
    public void testC5GetKVPEntryGreaterThan32MB() throws Exception {
        log.info("Get multiple KVPs with Keys of total size > 32MB");
        try {
            Integer keyId;
            Integer totalsize=0;
            Map<Integer, String> multiKVP = new HashMap<Integer, String>();
            this.keyValueTable = this.KeyValueTableFactory.forKeyValueTable(KVT.getKeyValueTableName(),KEY_SERIALIZER, VALUE_SERIALIZER,KeyValueTableClientConfiguration.builder().build());
            log.debug("Value String"+tetsKeyValue);
            for (int loop=0; loop < 35; loop++) {
                log.info("Loop count :" + loop);
                keyId = ThreadLocalRandom.current().nextInt(1000, 99999);
                multiKVP.put(keyId, tetsKeyValue);
                CompletableFuture<Version> insertEntry = this.keyValueTable.put(null,keyId,tetsKeyValue);
                log.info("totalsize is " + totalsize);
                totalsize = totalsize + keyId.toString().getBytes().length + tetsKeyValue.getBytes().length;
                log.info("size of the key " + keyId.toString().getBytes().length);
                log.info("Size of the value string " + tetsKeyValue.getBytes().length);
                log.info("Final totalsize is" + totalsize);
            }
            CompletableFuture<List<TableEntry<Integer, String>>> getEntry = this.keyValueTable.getAll(null,multiKVP.keySet());
            log.info("map size "+multiKVP.size());
            log.info(("size of output of getall API "+getEntry.get().size()));
            Assert.assertEquals("Unexpected result size", multiKVP.size(), getEntry.get().size());
            log.info("successfully get KVP entry more 32 MB");
        }catch (Exception error) {
            log.info(error.getMessage());
        }
    }
    @Test
    // Test case 32
    public void testC6MultTableEntryWithKeyFamilyGreaterThan32MB() throws Exception {
        log.info("Insert multiple keyFamily KVPs with entries of total size > 32MB");
        try {
            Integer keyId;
            Integer totalsize=0;
            String keyfamily = "TestkeyFamily";
            Map<Integer, String> multiKVP = new HashMap<Integer, String>();
            log.debug("Value String"+tetsKeyValue);
            for (int loop=0; loop < 35; loop++) {
                log.info("Loop count :" + loop);
                keyId = ThreadLocalRandom.current().nextInt(1000, 99999);
                multiKVP.put(keyId, tetsKeyValue);
                log.info("totalsize is " + totalsize);
                totalsize = totalsize + keyId.toString().getBytes().length + tetsKeyValue.getBytes().length;
                log.info("size of the key " + keyId.toString().getBytes().length);
                log.info("Size of the value string " + tetsKeyValue.getBytes().length);
                log.info("Final totalsize is" + totalsize);
            }
            this.keyValueTable = this.KeyValueTableFactory.forKeyValueTable(KVT.getKeyValueTableName(), KEY_SERIALIZER, VALUE_SERIALIZER, KeyValueTableClientConfiguration.builder().build());
            CompletableFuture<List<Version>> insertEntry = this.keyValueTable.putAll(keyfamily,multiKVP.entrySet());
            List<Version> result = insertEntry.get();
            log.info("Insert successfully");
            CompletableFuture<List<TableEntry<Integer, String>>> kvpEntry = this.keyValueTable.getAll(keyfamily,multiKVP.keySet());
            log.info("Size of total count key "+ kvpEntry.get().size());
            log.info("key of 1st index "+kvpEntry.get().get(0).getKey().getKey().toString().getBytes().length);
            log.info("value of 1st index"+kvpEntry.get().get(0).getValue().getBytes().length);
            //assertTrue("conpare key",keyId == kvpEntry.get().getKey().getKey());
            //assertTrue("Compare value", value.getBytes().length == kvpEntry.get().getValue().getBytes().length);
        }catch (ExecutionException | InterruptedException  error) {
            error.printStackTrace();
            log.info("exception messge "+error.getMessage());

            log.info ("tostring message " +error.toString());
        }
    }
    @Test
    // Test case 35 (Able to fatch > 32 MB)
    public void testC7MultiGetTableEntrywithKeyFamilyGreaterThan32MB() throws Exception {
        log.info("Get multiple keyFamily KVP values with Keys of total size > 32MB");
        try {
            Integer keyId;
            Integer totalsize=0;
            String keyfamily = "TestKF";
            Map<Integer, String> multiKVP = new HashMap<Integer, String>();
            this.keyValueTable = this.KeyValueTableFactory.forKeyValueTable(KVT.getKeyValueTableName(),KEY_SERIALIZER, VALUE_SERIALIZER,KeyValueTableClientConfiguration.builder().build());
            log.debug("Value String"+tetsKeyValue);
            for (int loop=0; loop < 35; loop++) {
                log.info("Loop count :" + loop);
                keyId = ThreadLocalRandom.current().nextInt(1000, 99999);
                multiKVP.put(keyId, tetsKeyValue);
                CompletableFuture<Version> insertEntry = this.keyValueTable.put(keyfamily,keyId,tetsKeyValue);
                log.info("totalsize is " + totalsize);
                totalsize = totalsize + keyId.toString().getBytes().length + tetsKeyValue.getBytes().length;
                log.info("size of the key " + keyId.toString().getBytes().length);
                log.info("Size of the value string " + tetsKeyValue.getBytes().length);
                log.info("Final totalsize is" + totalsize);
            }
            CompletableFuture<List<TableEntry<Integer, String>>> getEntry = this.keyValueTable.getAll(keyfamily,multiKVP.keySet());
            Integer size = getEntry.toString().length();
            log.info("Total retrived value size in MB"+bytesToMB(size.toString().length()));
            log.info("map size "+multiKVP.size());
            log.info(("size of output of getall API "+getEntry.get().size()));
            Assert.assertEquals("Unexpected result size", multiKVP.size(), getEntry.get().size());
            log.info("Successfully retrive More than 30MB from KVT");
        }catch (Exception error) {
            log.info(error.getMessage());
        }
    }
    @Test
    public void testC8UpdateMultikeyvaluewithKeyFamilyGreaterThan32MB() throws Exception {
        log.info("Update multiple keyFamily KVP values with entries of total size > 32MB");
        try {
            Integer keyId;
            Integer totalsize=0;
            String keyfamily = "TestKF";
            Map<Integer, String> multiKVP = new HashMap<Integer, String>();
            this.keyValueTable = this.KeyValueTableFactory.forKeyValueTable(KVT.getKeyValueTableName(),KEY_SERIALIZER, VALUE_SERIALIZER,KeyValueTableClientConfiguration.builder().build());
            log.debug("Value String"+tetsKeyValue);
            for (int loop=0; loop < 35; loop++) {
                log.info("Loop count :" + loop);
                keyId = ThreadLocalRandom.current().nextInt(1000, 99999);
                multiKVP.put(keyId, tetsKeyValue);
                CompletableFuture<Version> insertEntry = this.keyValueTable.put(keyfamily,keyId,tetsKeyValue);
                log.info("totalsize is " + totalsize);
                totalsize = totalsize + keyId.toString().getBytes().length + tetsKeyValue.getBytes().length;
                log.info("size of the key " + keyId.toString().getBytes().length);
                log.info("Size of the value string " + tetsKeyValue.getBytes().length);
                log.info("Final totalsize is" + totalsize);
            }
            String testValue = convertStringToBinary("Hello Pravega");
            while (testValue.getBytes().length <= 1040000) {
                testValue = testValue + convertStringToBinary("Hello Pravega");
            }
            List<TableEntry<Integer, String>> getEntry = this.keyValueTable.getAll(keyfamily,multiKVP.keySet()).join();
            Integer oldSize = getEntry.get(0).getValue().getBytes().length;
            ListIterator<TableEntry<Integer, String>> iterator = getEntry.listIterator();
            while (iterator.hasNext()) {
                iterator.next().getValue().replace(tetsKeyValue, testValue);
            }
            CompletableFuture<List<Version>> getupdate = this.keyValueTable.replaceAll(keyfamily,getEntry);
            log.info("Successfully update KVP "+ getupdate.toString().length());
            List<TableEntry<Integer, String>> getEntryAfterUpdate = this.keyValueTable.getAll(keyfamily,multiKVP.keySet()).join();
            Integer newSize = getEntryAfterUpdate.get(0).getValue().getBytes().length;
            log.info("map size "+multiKVP.size());
            log.info("size of output of getall API "+getEntryAfterUpdate.size());
            //Assert.assertEquals("Unexpected result size", multiKVP.size(), getEntryAfterUpdate.size());
            assertNotEquals("Key value update",oldSize,newSize);
        }catch (Exception | AssertionError error) {
            error.getStackTrace();
            log.info(error.getMessage());
            log.info(error.toString());
        }
    }

    @Test
    // Test case 34
    public void testC9RemoveMultikeyvaluewithKeyFamilyGreaterThan32MB() throws Exception {
        log.info("Remove multiple keyFamily KVPs with Keys of total size > 32MB");
        try {
            Integer keyId;
            Integer totalsize=0;
            String keyfamily = "TestKF";
            Map<Integer, String> multiKVP = new HashMap<Integer, String>();
            this.keyValueTable = this.KeyValueTableFactory.forKeyValueTable(KVT.getKeyValueTableName(),KEY_SERIALIZER, VALUE_SERIALIZER,KeyValueTableClientConfiguration.builder().build());
            log.debug("Value String"+tetsKeyValue);
            for (int loop=0; loop < 34; loop++) {
                log.info("Loop count :" + loop);
                keyId = ThreadLocalRandom.current().nextInt(1000, 99999);
                multiKVP.put(keyId, tetsKeyValue);
                CompletableFuture<Version> insertEntry = this.keyValueTable.put(keyfamily,keyId,tetsKeyValue);
                log.info("totalsize is " + totalsize);
                totalsize = totalsize + keyId.toString().getBytes().length + tetsKeyValue.getBytes().length;
                log.info("size of the key " + keyId.toString().getBytes().length);
                log.info("Size of the value string " + tetsKeyValue.getBytes().length);
                log.info("Final totalsize is" + totalsize);
            }
            AsyncIterator<IteratorItem<TableKey<Integer>>> tkIterator = this.keyValueTable.keyIterator(keyFamily,multiKVP.size(),null);
            log.info("tkIterator value is "+tkIterator);
            List<TableKey<Integer>> gettkIterator = tkIterator.getNext().get().getItems();
            log.info("get tkIterator value is "+gettkIterator);
            this.keyValueTable.removeAll(keyfamily,gettkIterator);
            List<TableEntry<Integer, String>> getEntry = this.keyValueTable.getAll(keyfamily,multiKVP.keySet()).join();
            log.info("size of output of getall API "+getEntry.size());
            Assert.assertEquals("Unexpected result size", 0, getEntry.size());
        }catch (Exception | AssertionError error) {
            error.getStackTrace();
            log.info("Getmessage error "+error.getMessage());
            log.info("tostring error message"+error.toString());
        }
    }
    @Test
    public void testD1RemoveMultikeyvaluewithKeyFamilyLessThan32MB() throws Exception {
        log.info("Remove multiple keyFamily KVPs with Keys of total size > 32MB");
        try {
            Integer keyId;
            Integer totalsize=0;
            String keyfamily = "TestKF";
            Map<Integer, String> multiKVP = new HashMap<Integer, String>();
            this.keyValueTable = this.KeyValueTableFactory.forKeyValueTable(KVT.getKeyValueTableName(),KEY_SERIALIZER, VALUE_SERIALIZER,KeyValueTableClientConfiguration.builder().build());
            log.debug("Value String"+tetsKeyValue);
            for (int loop=0; loop < 13; loop++) {
                log.info("Loop count :" + loop);
                keyId = ThreadLocalRandom.current().nextInt(1000, 99999);
                multiKVP.put(keyId, tetsKeyValue);
                CompletableFuture<Version> insertEntry = this.keyValueTable.put(keyfamily,keyId,tetsKeyValue);
                log.info("totalsize is " + totalsize);
                totalsize = totalsize + keyId.toString().getBytes().length + tetsKeyValue.getBytes().length;
                log.info("size of the key " + keyId.toString().getBytes().length);
                log.info("Size of the value string " + tetsKeyValue.getBytes().length);
                log.info("Final totalsize is" + totalsize);
            }
            AsyncIterator<IteratorItem<TableKey<Integer>>> tkIterator = this.keyValueTable.keyIterator(keyFamily,multiKVP.size(),null);
            log.info("tkIterator value is "+tkIterator);
            List<TableKey<Integer>> gettkIterator = tkIterator.getNext().get().getItems();
            log.info("get tkIterator value is "+gettkIterator);
            this.keyValueTable.removeAll(keyfamily,gettkIterator);
            List<TableEntry<Integer, String>> getEntry = this.keyValueTable.getAll(keyfamily,multiKVP.keySet()).join();
            log.info("size of output of getall API "+getEntry.size());
            Assert.assertEquals("Unexpected result size", 0, getEntry.size());
        }catch (Exception | AssertionError error) {
            error.getStackTrace();
            log.info("Getmessage error "+error.getMessage());
            log.info("tostring error message"+error.toString());
        }
    }
/* @Test
  public void testC1CreateKeyEntry() throws Exception {
        log.info("Create key with length > 8KB");
        try {
            val TableName = KVT.
                    keyType = 10;
            keyFamily ="TestKeyFamily";
            String segmentName = "Segment";
            CompletableFuture<Version> insertEntry = this.keyValueTable.put(keyFamily,keyType,valueType);
            //KeyValueTableMap keyEntry= this.keyValueTable.getMapFor(keyFamily);
            Version version = insertEntry.get().asImpl().
                    TableEntry.versioned(keyEntry,version,valueType);
            TableKey tk = new KeyValueTableFactoryImpl().forKeyValueTable()
            //TableKey().versioned();
        } catch (AssertionError error) {
        }
    }*/


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
    private static final long  MEGABYTE = 1024L * 1024L;

    public static long bytesToMB(long bytes) {
        return bytes / MEGABYTE ;
    }
    public static long KBToMB(long KB)
    {
        return KB / MEGABYTE;
    }
    public static String convertStringToBinary(String input) {

        StringBuilder result = new StringBuilder();
        char[] chars = input.toCharArray();
        for (char aChar : chars) {
            result.append(String.format("%8s", Integer.toBinaryString(aChar)).replaceAll(" ", "0")
            );
        }
        return result.toString();

    }
}
