/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.nautilus.stream.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

import com.emc.nautilus.stream.Serializer;

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
