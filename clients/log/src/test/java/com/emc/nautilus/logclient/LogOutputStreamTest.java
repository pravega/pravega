package com.emc.nautilus.logclient;

import static org.junit.Assert.fail;

import java.util.UUID;

import org.junit.Test;

import com.emc.nautilus.common.netty.client.ConnectionFactoryImpl;
import com.emc.nautilus.logclient.impl.SegmentOutputStreamImpl;

public class LogOutputStreamTest {

	SegmentOutputStreamImpl createOutputStream() {
		return new SegmentOutputStreamImpl(new ConnectionFactoryImpl(false, 10000),
				"localhost",
				UUID.randomUUID(),
				"segmemt");
	}

	@Test
	public void testAckCallback() {
		fail();
	}

	@Test
	public void testClose() {
		fail();
	}

	@Test
	public void testFlush() {
		fail();
	}

	@Test
	public void testAutoClose() {
		fail();
	}

	@Test
	public void testFailOnAutoClose() {
		fail();
	}

	@Test
	public void testOutOfOrderAcks() {
		fail();
	}

	@Test
	public void testLargeWrite() {
		fail();
	}
}
