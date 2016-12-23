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
package com.emc.pravega.controller.server;

import static com.emc.pravega.controller.util.Config.ASYNC_TASK_POOL_SIZE;
import static com.emc.pravega.controller.util.Config.HOST_STORE_TYPE;
import static com.emc.pravega.controller.util.Config.STREAM_STORE_TYPE;
import static com.emc.pravega.controller.util.Config.STORE_TYPE;

import com.emc.pravega.controller.fault.SegmentContainerMonitor;
import com.emc.pravega.controller.fault.UniformContainerBalancer;
//import com.emc.pravega.controller.server.rest.RESTServer;
import com.emc.pravega.controller.server.rest.resources.Name;
import com.emc.pravega.controller.server.rpc.RPCServer;
import com.emc.pravega.controller.server.rpc.v1.ControllerServiceAsyncImpl;
import com.emc.pravega.controller.store.StoreClient;
import com.emc.pravega.controller.store.StoreClientFactory;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.host.HostStoreFactory;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.StreamStoreFactory;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.store.task.TaskStoreFactory;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.emc.pravega.controller.task.TaskSweeper;
import com.emc.pravega.controller.util.Config;
import com.emc.pravega.controller.util.ZKUtils;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;
//import org.eclipse.jetty.webapp.Configuration;
import org.eclipse.jetty.xml.XmlConfiguration;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import javax.ws.rs.core.Application;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Entry point of controller server.
 */
@Slf4j
public class Main {

    public static void main(String[] args) throws Exception {
        String hostId;
        try {
            //On each controller process restart, it gets a fresh hostId,
            //which is a combination of hostname and random GUID.
            hostId = InetAddress.getLocalHost().getHostAddress() + UUID.randomUUID().toString();
        } catch (UnknownHostException e) {
            log.debug("Failed to get host address.", e);
            hostId = UUID.randomUUID().toString();
        }

        //1. LOAD configuration.
        //Initialize the executor service.
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(ASYNC_TASK_POOL_SIZE,
                new ThreadFactoryBuilder().setNameFormat("taskpool-%d").build());

        log.info("Creating store client");
        StoreClient storeClient = StoreClientFactory.createStoreClient(
                StoreClientFactory.StoreType.valueOf(STORE_TYPE));

        log.info("Creating the stream store");
        StreamMetadataStore streamStore = StreamStoreFactory.createStore(
                StreamStoreFactory.StoreType.valueOf(STREAM_STORE_TYPE), executor);

        log.info("Creating zk based task store");
        TaskMetadataStore taskMetadataStore = TaskStoreFactory.createStore(storeClient, executor);

        log.info("Creating the host store");
        HostControllerStore hostStore = HostStoreFactory.createStore(
                HostStoreFactory.StoreType.valueOf(HOST_STORE_TYPE));

        //Host monitor is not required for a single node local setup.
        if (Config.HOST_MONITOR_ENABLED) {
            //Start the Segment Container Monitor.
            log.info("Starting the segment container monitor");
            SegmentContainerMonitor monitor = new SegmentContainerMonitor(hostStore,
                    ZKUtils.CuratorSingleton.CURATOR_INSTANCE.getCuratorClient(), Config.CLUSTER_NAME,
                    new UniformContainerBalancer(), Config.CLUSTER_MIN_REBALANCE_INTERVAL);
            monitor.startAsync();
        }

        //2. Start the RPC server.
        log.info("Starting RPC server");
        StreamMetadataTasks streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore, taskMetadataStore,
                executor, hostId);
        StreamTransactionMetadataTasks streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore,
                hostStore, taskMetadataStore, executor, hostId);
        RPCServer.start(new ControllerServiceAsyncImpl(streamStore, hostStore, streamMetadataTasks,
                streamTransactionMetadataTasks));

        //3. Hook up TaskSweeper.sweepOrphanedTasks as a callback on detecting some controller node failure.
        // todo: hook up TaskSweeper.sweepOrphanedTasks with Failover support feature
        // Controller has a mechanism to track the currently active controller host instances. On detecting a failure of
        // any controller instance, the failure detector stores the failed HostId in a failed hosts directory (FH), and
        // invokes the taskSweeper.sweepOrphanedTasks for each failed host. When all resources under the failed hostId
        // are processed and deleted, that failed HostId is removed from FH folder.
        // Moreover, on controller process startup, it detects any hostIds not in the currently active set of
        // controllers and starts sweeping tasks orphaned by those hostIds.
        TaskSweeper taskSweeper = new TaskSweeper(taskMetadataStore, hostId, streamMetadataTasks,
                streamTransactionMetadataTasks);

        // 4. start REST server

        /*//RESTServer restServer = new RESTServer();
        ResourceConfig config = new ResourceConfig(Name.class);
        //config.packages("pravega");
        config.packages("com.emc.pravega.controller.server.rest");
        ServletHolder servlet = new ServletHolder(new ServletContainer(config));

        Server server = new Server(2222);
        ServletContextHandler context = new ServletContextHandler(server, "*//*");
        context.addServlet(servlet, "*//*");

        log.info("Starting REST server");
        try {
            server.start();
            server.join();
        } catch (Exception e) {
            log.info("Exception = {}", e.toString());
        } finally {
            server.destroy();
        }*/

        // code from image repository
        log.info("Initializing REST Service");

        // 1 create an application with the desired resources.
        final Application application = createApplication(new Class[] { Name.class });

        // 2 Create frontend
        final Server server = createJettyServer(application);
        server.start();

        log.info("Pravega REST Service has started");
        // 3 wait for ever.
        server.join();

    }

    private static final Server createJettyServer(Application application) throws Exception {

        Resource serverConfig = Resource.newResource("/root/pravega_rest/pravega/controller/server/src/conf/jetty_server.xml");
        ///root/pravega_rest/pravega/controller/server/src/conf/jetty_server.xml
        XmlConfiguration configuration = new XmlConfiguration(serverConfig.getInputStream());
        Server server = (Server) configuration.configure();

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        final ResourceConfig resourceConfig = ResourceConfig.forApplication(application);
        //resourceConfig.registerClasses(providerClasses);

        ServletHolder jerseyServlet = new ServletHolder(new ServletContainer(resourceConfig));

        jerseyServlet.setInitOrder(0);
        context.addServlet(jerseyServlet, "/*");

        server.setHandler(context);
        return server;
    }

    private static final Application createApplication(Class<?>[] classes) {
        // create resources.
        final Set<Object> resources = new HashSet<Object>();
        for (Class<?> resource : classes) {
            try {
                resources.add(resource.newInstance());
            } catch (InstantiationException | IllegalAccessException e) {
                log.error("Error during instantiation of REST resource : {} ", resource.getName(), e);
            }
        }
        return new Resources(resources);
    }

    //private static final Class<?> providerClasses[] = { };

    //private static final String SECTION = "server";
    //private static final String JETTY_CONFIG_FILE = Configuration.make(String.class, SECTION + ".jettyConfigurationFile","conf/jetty_server.xml").value();
}
