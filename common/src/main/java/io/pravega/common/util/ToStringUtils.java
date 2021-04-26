/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.common.util;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import io.pravega.common.Exceptions;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toMap;

public class ToStringUtils {

    /**
     * Transforms a map into a string of the from:
     * "K1=V1, K2=V2, K3=V3"
     * Where the string versions of the key and value are derived from their toString() function.
     * 
     * @param <K> The type of the keys of the map.
     * @param <V> The type of the values of the map.
     * @param map The map to be serialized to a string
     * @return A string representation of the map.
     */
    public static <K, V> String mapToString(Map<K, V> map) {
        Map<String, String> asStrings = map.entrySet()
                                           .stream()
                                           .collect(toMap(e -> e.getKey().toString(), e -> e.getValue().toString()));
        asStrings.forEach((k, v) -> {
            Preconditions.checkArgument(k == null || !((k.contains(",") || k.contains("="))), "Invalid key: %s", k);
            Preconditions.checkArgument(v == null || !((v.contains(",") || v.contains("="))), "Invalid value: %s", v);
        });
        return Joiner.on(", ").withKeyValueSeparator('=').join(asStrings);
    }

    /**
     * Performs the reverse of {@link #mapToString(Map)}. It parses a map written in a string form
     * back into a Java map.
     * 
     * Note that in order to parse properly, it is important that none the keys or values that
     * were serialized contain '=' or ',' characters as this prevents parsing. For this reason it
     * should be noted that this simple format does not support nesting.
     * 
     * @param <K> The type of the keys of the map.
     * @param <V> The type of the values of the map.
     * @param serialized The serialized form of the map.
     * @param keyMaker The constructor for the key objects
     * @param valueMaker The constructor for the value objects
     * @return A map the corresponds to the serialized string.
     */
    public static <K, V> Map<K, V> stringToMap(String serialized, Function<String, K> keyMaker,
                                               Function<String, V> valueMaker) {
        Map<String, String> map = Splitter.on(',').trimResults().withKeyValueSeparator('=').split(serialized);
        return map.entrySet()
                  .stream()
                  .collect(toMap(e -> keyMaker.apply(e.getKey()), e -> valueMaker.apply(e.getValue())));
    }

    /**
     * Transforms a list into a string of the from:
     * "V1,V2,V3"
     * Where the string versions of the key and value are derived from their toString() function.
     *
     * @param <V> The type of the values of the list.
     * @param list The list to be serialized to a string
     * @return A string representation of the list.
     */
    public static <V> String listToString(List<V> list) {
        List<String> asStrings = list.stream().map(Object::toString).collect(Collectors.toList());
        asStrings.forEach(v -> {
            Preconditions.checkArgument(v == null || !v.contains(","), "Invalid value: %s", v);
        });
        return Joiner.on(",").join(asStrings);
    }

    /**
     * Performs the reverse of {@link #listToString(List)}. It parses a list written in a string form
     * back into a Java list.
     *
     * Note that in order to parse properly, it is important that none of the values that
     * were serialized contain ',' character as this prevents parsing. For this reason it
     * should be noted that this simple format does not support nesting.
     *
     * @param <V> The type of the values of the list.
     * @param serialized The serialized form of the list.
     * @param valueMaker The constructor for the value objects
     * @return A list that corresponds to the serialized string.
     */
    public static <V> List<V> stringToList(String serialized, Function<String, V> valueMaker) {
        List<String> list = Splitter.on(',').trimResults().splitToList(serialized);
        return list.stream().map(valueMaker::apply).collect(Collectors.toList());
    }

    /**
     * Convert the given string to its compressed base64 representation.
     * @param string String to be compressed to base64.
     * @return String Compressed Base64 representation of the input string.
     * @throws NullPointerException If string is null.
     */
    @SneakyThrows(IOException.class)
    public static String compressToBase64(final String string) {
        Preconditions.checkNotNull(string, "string");
        @Cleanup
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        @Cleanup
        final OutputStream base64OutputStream = Base64.getEncoder().wrap(byteArrayOutputStream);
        @Cleanup
        final GZIPOutputStream gzipOutputStream = new GZIPOutputStream(base64OutputStream);
        gzipOutputStream.write(string.getBytes(UTF_8));
        gzipOutputStream.close();
        return byteArrayOutputStream.toString(UTF_8.name());
    }

    /**
     * Get the original string from its compressed base64 representation.
     * @param base64CompressedString Compressed Base64 representation of the string.
     * @return The original string.
     * @throws NullPointerException If base64CompressedString is null.
     * @throws IllegalArgumentException If base64CompressedString is not null, but has a length of zero or if the input is invalid.
     */
    @SneakyThrows(IOException.class)
    public static String decompressFromBase64(final String base64CompressedString) {
        Exceptions.checkNotNullOrEmpty(base64CompressedString, "base64CompressedString");
        try {
            @Cleanup
            final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(base64CompressedString.getBytes(UTF_8));
            @Cleanup
            final InputStream base64InputStream = Base64.getDecoder().wrap(byteArrayInputStream);
            @Cleanup
            final GZIPInputStream gzipInputStream = new GZIPInputStream(base64InputStream);
            return IOUtils.toString(gzipInputStream, UTF_8);
        } catch (ZipException | EOFException e) { // exceptions thrown for invalid encoding and partial data.
            throw new IllegalArgumentException("Invalid base64 input.", e);
        }
    }

}
