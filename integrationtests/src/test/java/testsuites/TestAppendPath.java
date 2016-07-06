package testsuites;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.emc.logservice.serverhost.handler.AppendProcessorTest;
import com.emc.nautilus.common.netty.AppendEncodeDecodeTest;
import com.emc.nautilus.integrationtests.AppendTest;
import com.emc.nautilus.logclient.SegmentOutputStreamTest;

@RunWith(Suite.class)
@SuiteClasses({AppendEncodeDecodeTest.class, AppendProcessorTest.class, AppendTest.class, SegmentOutputStreamTest.class})
public class TestAppendPath {

}
