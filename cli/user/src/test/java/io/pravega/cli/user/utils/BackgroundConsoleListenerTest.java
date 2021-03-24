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
package io.pravega.cli.user.utils;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;

public class BackgroundConsoleListenerTest {

    @Test
    public void testBackgroundConsoleListener() throws InterruptedException {
        BackgroundConsoleListener listener = new BackgroundConsoleListener();
        Assert.assertFalse(listener.isTriggered());
        listener.start();
        Assert.assertFalse(listener.isTriggered());
        Thread.sleep(1000);
        System.setIn(new ByteArrayInputStream("test input\r\n".getBytes()));
        Thread.sleep(1000);
        System.setIn(new ByteArrayInputStream("quit\r\n".getBytes()));
        listener.stop();
        Assert.assertTrue(listener.isTriggered());
        listener.close();
    }
}
