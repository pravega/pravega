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
package io.pravega.test.system.framework.services.kubernetes;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.introspect.VisibilityChecker;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Wrapper class for deserializing the json file contents.
 */
@Slf4j
@Getter
@Setter
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonDeserialize
public class ResourceWrapper {
    private BookkeeperProperties bookkeeperProperties;
    private ControllerProperties controllerProperties;
    private SegmentStoreProperties segmentStoreProperties;
    private ZookeeperProperties zookeeperProperties;
    private Map<String, String> pravegaOptions;

    public static ResourceWrapper getSystemTestConfig(String configFile) {
        JsonFactory jsonFactory = new JsonFactory();
        jsonFactory.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
        jsonFactory.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
        ObjectMapper objectMapper = new ObjectMapper(jsonFactory);
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        objectMapper.setVisibility(VisibilityChecker.Std.defaultInstance().withFieldVisibility(JsonAutoDetect.Visibility.ANY));
        objectMapper.enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);
        InputStream stream = ResourceWrapper.class.getClassLoader().getResourceAsStream(configFile);
        ResourceWrapper resourceWrapper = null;

        try {
            resourceWrapper = objectMapper.readValue(stream, ResourceWrapper.class);
        } catch (IOException e) {
            log.error("Input json file not available", e);
        } finally {
            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return resourceWrapper;
    }
}
