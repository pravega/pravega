/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.connectors.flink;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.stream.mock.MockClientFactory;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URI;
import java.nio.ByteBuffer;

public class FlinkPravegaWriterTest {

    @Before
    public void setup() {

    }



    @Test
    public void TestWriter() throws Exception {

        /*
        final Configuration flinkConfig = new Configuration();
        flinkConfig.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);
        flinkConfig.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 8);
        flinkConfig.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 16);
        */

        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.createLocalEnvironment();

        DataStreamSource<String> dataStream = execEnv.addSource(new RichParallelSourceFunction<String>() {
            private boolean running = true;

            @Override public void run(SourceContext<String> ctx) throws Exception {
                long seq = 0;
                while (running) {
                    Thread.sleep(500);
                    ctx.collect((seq++) + "-" + RandomStringUtils.randomAlphabetic(12));
                }
            }

            @Override public void cancel() {
                running = false;
            }
        });

        MockClientFactory clientFactory = new MockClientFactory("Scope", "localhost", 9090);
        clientFactory.createStream("Foo", null);

        FlinkPravegaWriter<String> pravegaSink = new FlinkPravegaWriter<String>(
                URI.create("tcp://localhost"),
                "Scope",
                "Foo",
                new SimpleStringSchema(),
                event -> "test");
        pravegaSink.setPravegaWriterMode(PravegaWriterMode.ATLEAST_ONCE);

        dataStream.setParallelism(1).addSink(pravegaSink);

        execEnv.execute();

    }
}
