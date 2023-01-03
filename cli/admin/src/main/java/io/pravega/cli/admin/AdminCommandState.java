/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.cli.admin;

import io.pravega.cli.admin.serializers.AbstractSerializer;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Getter;
import lombok.Setter;

import static io.pravega.cli.admin.utils.ConfigUtils.getIfEnv;

/**
 * Keeps state between commands.
 */
public class AdminCommandState implements AutoCloseable {
    @Getter
    private final ServiceBuilderConfig.Builder configBuilder;
    @Getter
    private final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(2, "admin-tools");
    @Getter
    @Setter
    private AbstractSerializer keySerializer = null;
    @Getter
    @Setter
    private AbstractSerializer valueSerializer = null;

    /**
     * Creates a new instance of the AdminCommandState class.
     *
     * @throws IOException If unable to read specified config properties file (assuming it exists).
     */
    public AdminCommandState() throws IOException {
        this.configBuilder = ServiceBuilderConfig.builder();
        try {
            Properties properties = new Properties();
            try (FileReader reader = new FileReader(System.getProperty(ServiceBuilderConfig.CONFIG_FILE_PROPERTY_NAME, "conf/admin-cli.properties"))) {
                properties.load(reader);
                for (String propertyName: properties.stringPropertyNames()) {
                    properties.setProperty(propertyName, getIfEnv(properties.getProperty(propertyName)));
                }
            }
            this.configBuilder.include(properties);
        } catch (FileNotFoundException ex) {
            // Nothing to do here.
        }
    }

    @Override
    public void close() {
        ExecutorServiceHelpers.shutdown(this.executor);
    }
}
