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

package com.emc.pravega.controller.server.rest;

import com.emc.pravega.controller.server.rest.resources.ResourceImpl;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.apache.log4j.Level;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.xml.XmlConfiguration;

import org.glassfish.jersey.logging.LoggingFeature;
import org.glassfish.jersey.netty.httpserver.NettyHttpContainerProvider;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;


import javax.ws.rs.core.Application;
import javax.ws.rs.core.UriBuilder;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;


/**
 * Jetty REST server implementation. (Initial version)
 */
@Slf4j
public class RESTServer {

     public static final Server createJettyServer(Application application) throws Exception {

        Resource serverConfig = Resource.newResource("/root/pravega_rest/pravega/controller/server/src/conf/jetty_server.xml");
        ///root/pravega_rest/pravega/controller/server/src/conf/jetty_server.xml
        XmlConfiguration configuration = new XmlConfiguration(serverConfig.getInputStream());
        Server server = (Server) configuration.configure();

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        final ResourceConfig resourceConfig = ResourceConfig.forApplication(application);
        //resourceConfig.registerClasses(providerClasses);
        //resourceConfig.register(LoggingFeature.class);

        ServletHolder jerseyServlet = new ServletHolder(new ServletContainer(resourceConfig));

        jerseyServlet.setInitOrder(0);
        context.addServlet(jerseyServlet, "/*");

        server.setHandler(context);
        //disable jetty logs
        //org.apache.log4j.LogManager.getLogger("org.eclipse.jetty").setLevel(Level.INFO);

        // disable jetty server logs
         final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger("org.eclipse.jetty");
         if ((logger instanceof ch.qos.logback.classic.Logger)) {
             ch.qos.logback.classic.Logger logbackLogger = (ch.qos.logback.classic.Logger) logger;
             logbackLogger.setLevel(ch.qos.logback.classic.Level.ERROR);
         }

         return server;
    }

    public static final Channel createNettyServer(Application controllerApplication) throws Exception {

        /*Resource serverConfig = Resource.newResource("/root/pravega_rest/pravega/controller/server/src/conf/jetty_server.xml");

        XmlConfiguration configuration = new XmlConfiguration(serverConfig.getInputStream());
        Server server = (Server) configuration.configure();

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        final ResourceConfig resourceConfig = ResourceConfig.forApplication(application);

        ServletHolder jerseyServlet = new ServletHolder(new ServletContainer(resourceConfig));

        jerseyServlet.setInitOrder(0);
        context.addServlet(jerseyServlet, "*//*");

        server.setHandler(context);

        return server;*/

        URI baseUri = UriBuilder.fromUri("http://localhost/").port(9998).build();
        ResourceConfig resourceConfig = ResourceConfig.forApplication(controllerApplication);
        Channel server = NettyHttpContainerProvider.createServer(baseUri, resourceConfig, true);

        return server;
    }
}
