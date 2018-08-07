/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.io;
import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import lombok.Cleanup;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Base64;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Miscellaneous serialization helper utilities.
 */
public final class SerializationHelpers {
    /**
     * Serialize an object to a Base64 representation.
     * @param object Object to be serialized to Base64
     * @param <T> Type of object.
     * @return String representing the object.
     * @throws IOException Exception thrown by the underlying OutputStream.
     * @throws NullPointerException If object is null.
     */
    public static <T extends Serializable> String serializeBase64(final T object) throws SerializationException {
        Preconditions.checkNotNull(object, "object");
        final byte[] bytes;
        try {
            @Cleanup final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            @Cleanup final GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream);
            @Cleanup final ObjectOutputStream objectOutputStream = new ObjectOutputStream(gzipOutputStream);
            objectOutputStream.writeObject(object);
            objectOutputStream.close();
            bytes = byteArrayOutputStream.toByteArray();
        } catch (IOException ex) {
            throw new SerializationException("Exception while serializing object to base64 representation.", ex);
        }
        return Base64.getEncoder().encodeToString(bytes);
    }

    /**
     * De-serialize a Base64 representation of an object.
     * @param base64String Base64 representation of the object.
     * @param <T> Type of object.
     * @return The deserialized object.
     * @throws SerializationException Exception thrown during deserialization of base64 representation of an object.
     * @throws NullPointerException If base64String is null.
     * @throws IllegalArgumentException If base64String is not null, but has a length of zero or if the string has illegal base64 character.
     */
    public static <T extends Serializable> T deserializeBase64(final String base64String) throws SerializationException {
        Exceptions.checkNotNullOrEmpty(base64String, "base64String");
        try {
            byte[] dataBytes = Base64.getDecoder().decode(base64String);
            @Cleanup final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(dataBytes);
            @Cleanup final GZIPInputStream gzipInputStream = new GZIPInputStream(byteArrayInputStream);
            final ObjectInputStream objectInputStream = new ObjectInputStream(gzipInputStream);

            @SuppressWarnings({"unchecked"}) final T object = (T) objectInputStream.readObject();
            objectInputStream.close();
            return object;
        } catch (IOException | ClassNotFoundException ex) {
            throw new SerializationException("Exception during deserializataion.", ex);
        }
    }
}
