/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.test.integration.testsuites;

import com.emc.pravega.test.integration.AppendTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.emc.pravega.common.netty.AppendEncodeDecodeTest;
import com.emc.pravega.service.server.host.handler.AppendProcessorTest;
import com.emc.pravega.stream.impl.segment.SegmentOutputStreamTest;

@RunWith(Suite.class)
@SuiteClasses({ AppendEncodeDecodeTest.class, AppendProcessorTest.class, AppendTest.class, SegmentOutputStreamTest.class })
public class TestAppendPath {

}
