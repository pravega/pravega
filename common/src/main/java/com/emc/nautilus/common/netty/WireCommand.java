package com.emc.nautilus.common.netty;

import java.io.DataOutput;
import java.io.IOException;

public interface WireCommand {
	WireCommands.Type getType();
	void process(CommandProcessor cp);
	void writeFields(DataOutput out) throws IOException;
}