package com.emc.nautilus.streaming;

import java.nio.ByteBuffer;

public interface Serializer<Item> {
	public ByteBuffer serialize(Item value);

	public Item deserialize(ByteBuffer serializedValue);
}
