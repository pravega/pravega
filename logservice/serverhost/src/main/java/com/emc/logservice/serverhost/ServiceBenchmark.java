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

package com.emc.logservice.serverhost;

import ch.qos.logback.classic.LoggerContext;
import com.emc.logservice.server.mocks.InMemoryServiceBuilder;
import com.emc.logservice.server.service.ServiceBuilder;
import com.emc.logservice.server.service.ServiceBuilderConfig;
import com.emc.logservice.serverhost.benchmark.Benchmark;
import com.emc.logservice.serverhost.benchmark.RecoveryBenchmark;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Command-line program that benchmarks the StreamSegmentStore.
 */
public class ServiceBenchmark {

    public static void main(String[] args) {
        // Turn off all logging.
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        //context.getLoggerList().get(0).setLevel(Level.INFO);
        context.reset();

        ServiceBuilderConfig config = ServiceBuilderConfig.getDefaultConfig();
        //Supplier<ServiceBuilder> serviceBuilderProvider = ()-> new NoOpDataLogServiceBuilder(config);
        Supplier<ServiceBuilder> serviceBuilderProvider = () -> new InMemoryServiceBuilder(config);

        // WARNING: The benchmark does not work too well with DistributedLogServiceBuilder. In order to function
        //          properly, the benchmark needs to completely erase the DurableDataLog + Storage in order to ensure
        //          a clean test. This has not yet been implemented in the DistributedLog case, and there is no way
        //          to expose truncation outside of DurableLog. TODO Fix when we get that implemented.
        //Supplier<ServiceBuilder> serviceBuilderProvider = () -> new DistributedLogServiceBuilder(config);
        List<Benchmark> benchmarks = new ArrayList<>();
        benchmarks.add(new RecoveryBenchmark(serviceBuilderProvider));
        //benchmarks.add(new AppendsOnlyBenchmark(serviceBuilderProvider));

        for (Benchmark b : benchmarks) {
            b.run();
        }
    }

    //endregion
}
