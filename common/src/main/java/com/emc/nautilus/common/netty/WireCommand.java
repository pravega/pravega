package com.emc.nautilus.common.netty;

import java.io.DataOutput;
import java.io.IOException;

public interface WireCommand {
	WireCommands.Type getType();

	void writeFields(DataOutput out) throws IOException;
}
