package com.emc.nautilus.common.netty;

public interface Reply extends WireCommand {
	void process(ReplyProcessor cp);
}
