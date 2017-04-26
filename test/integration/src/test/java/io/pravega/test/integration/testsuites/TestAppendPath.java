/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.test.integration.testsuites;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import io.pravega.common.netty.AppendEncodeDecodeTest;
import io.pravega.test.integration.AppendTest;
import io.pravega.service.server.host.handler.AppendProcessorTest;
import io.pravega.stream.impl.segment.SegmentOutputStreamTest;

@RunWith(Suite.class)
@SuiteClasses({ AppendEncodeDecodeTest.class, AppendProcessorTest.class, AppendTest.class, SegmentOutputStreamTest.class })
public class TestAppendPath {

}
