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
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.VisibilityChecker;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;

@Slf4j
public class JSONReader {
    public static final String SYSTEMTESTPROPERTIES = "test/system/src/test/resources/systemTestConfig.json";
    public  static ResourceWrapper getSystemTestConfig() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        objectMapper.setVisibility(VisibilityChecker.Std.defaultInstance().withFieldVisibility(JsonAutoDetect.Visibility.ANY));
        objectMapper.enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);

        File file = new File(SYSTEMTESTPROPERTIES);

        ResourceWrapper resourceWrapperObj = null;
        try {
            log.info("*******" + SYSTEMTESTPROPERTIES);
            resourceWrapperObj = objectMapper.readValue(file, ResourceWrapper.class);
            log.info("*******" + resourceWrapperObj.getControllerProperties().getControllerResources().getRequests().get("cpu"));
            log.info("*******" + resourceWrapperObj.toString());
        } catch (IOException e) {
            log.error("Input json file not available", e);
        }
        return resourceWrapperObj;
    }
}
