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
package io.pravega.cli.user.config;

import org.junit.Assert;
import org.junit.Test;

public class InteractiveConfigCommandTest {

    @Test
    public void testSetConfig() {
        final String testString = "test";
        InteractiveConfig interactiveConfig = InteractiveConfig.getDefault();
        interactiveConfig.setControllerUri(testString);
        Assert.assertEquals(testString, interactiveConfig.getControllerUri());
        interactiveConfig.setTimeoutMillis(0);
        Assert.assertEquals(0, interactiveConfig.getTimeoutMillis());
        interactiveConfig.setMaxListItems(0);
        Assert.assertEquals(0, interactiveConfig.getMaxListItems());
        interactiveConfig.setDefaultSegmentCount(0);
        Assert.assertEquals(0, interactiveConfig.getDefaultSegmentCount());
        interactiveConfig.setPrettyPrint(false);
        Assert.assertFalse(interactiveConfig.isPrettyPrint());
        interactiveConfig.set(InteractiveConfig.AUTH_ENABLED, "true");
        Assert.assertTrue(interactiveConfig.isAuthEnabled());
        interactiveConfig.set(InteractiveConfig.CONTROLLER_USER_NAME, testString);
        Assert.assertEquals(testString, interactiveConfig.getUserName());
        interactiveConfig.set(InteractiveConfig.CONTROLLER_PASSWORD, testString);
        Assert.assertEquals(testString, interactiveConfig.getPassword());
        interactiveConfig.set(InteractiveConfig.TLS_ENABLED, "true");
        Assert.assertTrue(interactiveConfig.isTlsEnabled());
        interactiveConfig.set(InteractiveConfig.TRUSTSTORE_JKS, testString);
        Assert.assertEquals(testString, interactiveConfig.getTruststore());
        Assert.assertNotNull(interactiveConfig.getAll());
    }

}
