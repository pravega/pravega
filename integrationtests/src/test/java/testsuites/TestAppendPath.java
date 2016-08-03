package testsuites;

import com.emc.pravega.common.netty.AppendEncodeDecodeTest;
import com.emc.pravega.integrationtests.AppendTest;
import com.emc.pravega.service.server.host.handler.AppendProcessorTest;
import com.emc.pravega.stream.segment.impl.SegmentOutputStreamTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({AppendEncodeDecodeTest.class, AppendProcessorTest.class, AppendTest.class, SegmentOutputStreamTest.class})
public class TestAppendPath {

}
