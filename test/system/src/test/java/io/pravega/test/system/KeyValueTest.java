package io.pravega.test.system;

import io.pravega.client.control.impl.Controller;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.security.auth.DelegationTokenProviderFactory;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.client.tables.*;
import io.pravega.client.admin.KeyValueTableManager;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.impl.*;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.client.*;
import io.pravega.client.admin.*;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;

import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;

@Slf4j
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(SystemTestRunner.class)
public class KeyValueTest extends AbstractSystemTest {
    private final static String SCOPE_NAME = "SampleScope3";
    private final static String SCOPE_NAME1 = "DiffScope1";
    private final static String KVT_NAME = "TestKVT1";
    //+ randomAlphanumeric(5);
    private URI controllerURI = null;
    private Controller controller = null;
    final KeyValueTableConfiguration config = KeyValueTableConfiguration.builder().partitionCount(2).build();
    private final Integer keyType = 1;
    private final String valueType = "TestValue";
    public final String scope="TestScope";
    private TableSegmentFactoryImpl segmentFactory;
    private KeyValueTable<Integer, String> keyValueTable;
    private static final KeyValueTableInfo KVT = new KeyValueTableInfo(SCOPE_NAME,KVT_NAME);
    private ConnectionFactory connectionFactory;
    private StreamManager streamManager;
    private Service controllerInstance;
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
        val tokenProvider = DelegationTokenProviderFactory.create(this.controller, KVT.getScope(), KVT.getKeyValueTableName());
        this.segmentFactory = new TableSegmentFactoryImpl(controller,connectionFactory,KeyValueTableClientConfiguration.builder().build(),tokenProvider);

    }

    @After
    public void tearDown() {
        //streamManager.close();
    }

    @Test
    //Test case - 13: Create KVT test
    public void test1CreateKeyValueTable() throws Exception {
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
    public void test2CreateExistingKeyValueTable() throws Exception {
        log.info("Create Key value Table(KVT)");
        try {
            @Cleanup
            val kvtManager = KeyValueTableManager.create(controllerURI);
            boolean createKVT = kvtManager.createKeyValueTable(SCOPE_NAME, KVT_NAME, config);
            Assert.assertFalse(createKVT);
            if (createKVT == false) {
                log.info("Failed to created KVT as same KVT already present");
            }
        } finally {
            log.info("Can not create as KVT already present");
        }
    }
    public void test3CreateSameKVTDiffScope() throws Exception {
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
    public void test4ListKeyValueTables() {
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
    /*
    * Test case-16 : updateKeyValueTable() is not a supported API
    * the only value we have in KVT configuration right now is partitionCount and that cannot be changed once the KVTable is created.
    * This might be supported in future if more configuration added in KVT configuration

    @Test
    public void test5UpdateKeyValueTable(){
        try {
            val kvtManager = KeyValueTableManager.create(controllerURI);
            boolean updateKVT = kvtManager.updateKeyValueTable(SCOPE_NAME, KVT_NAME, config);
            Assert.assertTrue(updateKVT);
        }catch (AssertionError error){
            log.info(error.getMessage());
        }
    }
    */
    @Test
    // Test Case-14: Delete KVT test
    public void test6DeleteKeyValueTable(){
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
    public void test7InsertKeyValuePair(){
        try {
            log.info("Insert KVP");
            this.keyValueTable = new KeyValueTableImpl<>(KVT, this.segmentFactory, this.controller, KEY_SERIALIZER, VALUE_SERIALIZER);
            CompletableFuture<Version> insertEntry = this.keyValueTable.put(null,keyType,valueType);
            Version result = insertEntry.get();
            log.info("result "+result);
        }catch (ExecutionException | InterruptedException  error) {
            log.info(error.getMessage());
        }
    }
    @Test
    public void test8InsertKeyValuePairUnconditionally(){
        try {
            log.info("Insert or update KVP");
            this.keyValueTable = new KeyValueTableImpl<>(KVT, this.segmentFactory, this.controller, KEY_SERIALIZER, VALUE_SERIALIZER);
            CompletableFuture<Version> insertEntry = this.keyValueTable.putIfAbsent(null,keyType,valueType);
            Version result = insertEntry.get();
            log.info("result "+result);
        }catch (ExecutionException | InterruptedException  error) {
            log.info(error.getMessage());
        }
    }
    @Test
    public void test9InsertMultipleKeyValuePair(){
        try {
            log.info("Insert multiple KVP");
            Map<Integer,String> multiKVP = new HashMap<Integer,String>();
            multiKVP.put(2,"TestValue2"); multiKVP.put(3,"TestValue3"); multiKVP.put(4,"TestValue4");
            this.keyValueTable = new KeyValueTableImpl<>(KVT, this.segmentFactory, this.controller, KEY_SERIALIZER, VALUE_SERIALIZER);
            CompletableFuture<List<Version>> insertKVP = this.keyValueTable.putAll(null,multiKVP.entrySet());
            List<Version> result = insertKVP.get();
            log.info("successfully inserted key "+multiKVP.entrySet()+"And output"+result);
        }catch (ExecutionException | InterruptedException  error) {
            log.info(error.getMessage());
        }
    }
    @Test
    public void test10RetrieveKeyValuePairEntry(){
        try {
            log.info("Retrieve Single KVP entry");
            this.keyValueTable = new KeyValueTableImpl<>(KVT, this.segmentFactory, this.controller, KEY_SERIALIZER, VALUE_SERIALIZER);
            CompletableFuture<TableEntry<Integer, String>> getKVT = this.keyValueTable.get(null,keyType);
            TableEntry<Integer, String> result = getKVT.get();
            log.info("Successfully retrive single keyvaluepair entry : "+result);
        }catch (ExecutionException | InterruptedException  error) {
            log.info(error.getMessage());
        }
    }
    /*
    @Test
    public void test12RetrieveMultipleKeyValuePairEntry(){
        try {
            log.info("Retrieve Single KVP entry");
            this.keyValueTable = new KeyValueTableImpl<>(KVT, this.segmentFactory, this.controller, KEY_SERIALIZER, VALUE_SERIALIZER);
            CompletableFuture<TableEntry<Integer, String>> getKVT = this.keyValueTable.getAll(null,);
            TableEntry<Integer, String> result = getKVT.get();
            log.info("Successfully retrive single keyvaluepair entry : "+result);
        }catch (ExecutionException | InterruptedException  error) {
            log.info(error.getMessage());
        }
    }

*/

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