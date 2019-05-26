/**
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.watermark;

import io.pravega.client.stream.Serializer;
import io.pravega.shared.watermarks.Watermark;

import java.nio.ByteBuffer;

public class WatermarkSerializer implements Serializer<Watermark> {
    @Override
    public ByteBuffer serialize(Watermark value) {
        return value.toByteBuf();
    }

    @Override
    public Watermark deserialize(ByteBuffer serializedValue) {
        return Watermark.fromByteBuf(serializedValue);
    }
}
