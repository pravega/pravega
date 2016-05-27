package com.emc.nautilus.common.netty;

public interface Request extends WireCommand {
	void process(RequestProcessor cp);
}
