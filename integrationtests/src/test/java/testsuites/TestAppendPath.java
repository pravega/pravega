package testsuites;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.emc.logservice.serverhost.handler.AppendProcessorTest;
import com.emc.nautilus.common.netty.AppendEncodeDecodeTest;
import com.emc.nautilus.integrationtests.AppendTest;
import com.emc.nautilus.logclient.LogOutputStreamTest;

@RunWith(Suite.class)
@SuiteClasses({AppendEncodeDecodeTest.class, AppendProcessorTest.class, AppendTest.class, LogOutputStreamTest.class})
public class TestAppendPath {

}
