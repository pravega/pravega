/**
 * Copyright (c) 2016 Dell Inc. or its subsidiaries. All Rights Reserved
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package  com.emc.pravega;

import com.emc.pravega.framework.Environment;
import com.emc.pravega.framework.SystemTestRunner;
import com.emc.pravega.framework.metronome.MetronomeClientNautilus;

import com.emc.pravega.framework.services.Service;
import com.emc.pravega.framework.services.ZookeeperService;
import mesosphere.marathon.client.utils.MarathonException;
import org.junit.BeforeClass;

import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.URI;

import static com.emc.pravega.framework.metronome.MetronomeClientNautilus.getClient;

@RunWith(SystemTestRunner.class)
public class ZookeeperTest {

    private static Service zk = new ZookeeperService("zookeeper");

    /*
        This is used to setup the various services required by the system test framework.
     */
    @Environment
    public static void setup() throws MarathonException {
        MetronomeClientNautilus.deleteAllJobs(getClient());
        if (!zk.isRunning()) {
            zk.start(true);
        }

    }

    @BeforeClass
    public static void beforeClass() {
        // This is the placeholder to perform any operation on the services before executing the system tests
    }

    /*
     * Invoke the producer test, ensure we are able to produce 100 messages to the stream.
     * The test fails incase of exceptions while writing to the stream.
     */

    @Test
    public void zkPingTest() {
        System.out.println("Start execution of zkPingTest");
        //Fetch the service details
        URI  zkUri = zk.getServiceDetails().get(0);
        System.out.println("zk Service URI: " + zkUri);
        System.out.println("Test execution completed");
    }

}
