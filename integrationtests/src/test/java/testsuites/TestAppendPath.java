package testsuites;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.emc.nautilus.stream.segment.impl.SegmentOutputStreamTest;
import com.emc.pravega.common.netty.AppendEncodeDecodeTest;
import com.emc.pravega.integrationtests.AppendTest;
import com.emc.pravega.service.server.host.handler.AppendProcessorTest;

@RunWith(Suite.class)
@SuiteClasses({AppendEncodeDecodeTest.class, AppendProcessorTest.class, AppendTest.class, SegmentOutputStreamTest.class})
public class TestAppendPath {

}
