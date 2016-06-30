package com.emc.nautilus.streaming.impl;

import java.io.*;
import java.nio.ByteBuffer;

import com.emc.nautilus.streaming.Serializer;

public class JavaSerializer<T extends Serializable> implements Serializer<T> {

	@Override
	public ByteBuffer serialize(T value) {
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		ObjectOutputStream oout;
		try {
			oout = new ObjectOutputStream(bout);
			oout.writeObject(value);
			oout.close();
			bout.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return ByteBuffer.wrap(bout.toByteArray());
	}

	@Override
	public T deserialize(ByteBuffer serializedValue) {
		ByteArrayInputStream bin = new ByteArrayInputStream(serializedValue.array(),
				serializedValue.position(),
				serializedValue.remaining());
		ObjectInputStream oin;
		try {
			oin = new ObjectInputStream(bin);
			return (T) oin.readObject();
		} catch (IOException | ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

}
