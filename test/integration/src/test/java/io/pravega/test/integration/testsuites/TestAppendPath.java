/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.testsuites;

import io.pravega.client.segment.impl.SegmentOutputStreamTest;
import io.pravega.service.server.host.handler.AppendProcessorTest;
import io.pravega.shared.protocol.netty.AppendEncodeDecodeTest;
import io.pravega.test.integration.AppendTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ AppendEncodeDecodeTest.class, AppendProcessorTest.class, AppendTest.class, SegmentOutputStreamTest.class })
public class TestAppendPath {

}
