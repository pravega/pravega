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
package io.pravega.cli.admin.serializers;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.pravega.client.stream.Serializer;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Base class for serializers.
 */
public abstract class AbstractSerializer implements Serializer<String> {

    public static final String STREAM_SEGMENT_RECORD_SEGMENT_NUMBER = "segmentNumber";
    public static final String STREAM_SEGMENT_RECORD_CREATION_EPOCH = "creationEpoch";
    public static final String STREAM_SEGMENT_RECORD_CREATION_TIME = "creationTime";
    public static final String STREAM_SEGMENT_RECORD_KEY_START = "keyStart";
    public static final String STREAM_SEGMENT_RECORD_KEY_END = "keyEnd";

    protected static final String EMPTY = "EMPTY";
    private static final String KEY_VALUE_SEPARATOR = ":";

    private static final Map<String, Function<StreamSegmentRecord, String>> STREAM_SEGMENT_RECORD_FIELD_MAP =
            ImmutableMap.<String, Function<StreamSegmentRecord, String>>builder()
                    .put(STREAM_SEGMENT_RECORD_SEGMENT_NUMBER, r -> String.valueOf(r.getSegmentNumber()))
                    .put(STREAM_SEGMENT_RECORD_CREATION_EPOCH, r -> String.valueOf(r.getCreationEpoch()))
                    .put(STREAM_SEGMENT_RECORD_CREATION_TIME, r -> String.valueOf(r.getCreationTime()))
                    .put(STREAM_SEGMENT_RECORD_KEY_START, r -> String.valueOf(r.getKeyStart()))
                    .put(STREAM_SEGMENT_RECORD_KEY_END, r -> String.valueOf(r.getKeyEnd()))
                    .build();

    /**
     * Method to return the name of the metadata being serialized.
     *
     * @return A string representing the metadata that is dealt with by this serializer.
     */
    public abstract String getName();

    /**
     * Append the given field name-value in a user-friendly format to the StringBuilder.
     *
     * @param builder The StringBuilder to append to.
     * @param name    The name of the field.
     * @param value   The value of the field.
     */
    public static void appendField(StringBuilder builder, String name, String value) {
        appendField(builder, name, value, ";", "=");
    }

    public static void appendField(StringBuilder builder, String name, String value, String pairDelimiter, String valueDelimiter) {
        builder.append(name).append(valueDelimiter).append(value).append(pairDelimiter);
    }

    /**
     * Parse the given string into a map of keys and values.
     *
     * @param stringData The string to parse
     * @return A map containing all the key-value pairs parsed from the string.
     */
    public static Map<String, String> parseStringData(String stringData) {
        return parseStringData(stringData, ";", "=");
    }

    public static Map<String, String> parseStringData(String stringData, String pairDelimiter, String valueDelimiter) {
        Map<String, String> parsedData = new LinkedHashMap<>();
        Arrays.stream(stringData.split(pairDelimiter)).forEachOrdered(kv -> {
            List<String> pair = Arrays.asList(kv.split(valueDelimiter));
            Preconditions.checkArgument(pair.size() == 2, String.format("Incomplete key-value pair provided in %s", kv));
            if (!parsedData.containsKey(pair.get(0))) {
                parsedData.put(pair.get(0), pair.get(1));
            }
        });
        return parsedData;
    }

    /**
     * Checks if the given key exists in the map. If it exists, it returns the value corresponding to the key and removes
     * the key from the map.
     *
     * @param data The map to check in.
     * @param key  The key.
     * @return The value of the key if it exists.
     * @throws IllegalArgumentException if the key does not exist.
     */
    public static String getAndRemoveIfExists(Map<String, String> data, String key) {
        String value = data.remove(key);
        Preconditions.checkArgument(value != null, String.format("%s not provided.", key));
        return value;
    }

    public static <T> String convertCollectionToString(Collection<T> collection, Function<T, String> converter) {
        return collection.isEmpty() ? EMPTY : collection.stream().map(converter).collect(Collectors.joining(","));
    }

    public static <T> Collection<T> convertStringToCollection(String collectionString, Function<String, T> converter) {
        return collectionString.equalsIgnoreCase(EMPTY) ? new ArrayList<>() : Arrays.stream(collectionString.split(",")).map(converter).collect(Collectors.toList());
    }

    public static <T, U> String convertMapToString(Map<T, U> map, Function<T, String> keyConverter, Function<U, String> valueConverter) {
        return map.isEmpty() ? EMPTY : convertCollectionToString(map.entrySet()
                .stream()
                .map(entry -> keyConverter.apply(entry.getKey()) + KEY_VALUE_SEPARATOR + valueConverter.apply(entry.getValue()))
                .collect(Collectors.toList()), s -> s);
    }

    public static <T, U> Map<T, U> convertStringToMap(String mapString, Function<String, T> keyConverter, Function<String, U> valueConverter, String name) {
        Map<T, U> map = new HashMap<>();
        if (!mapString.equalsIgnoreCase(EMPTY)) {
            convertStringToCollection(mapString, s -> s).forEach(s -> {
                List<String> pair = Arrays.asList(s.split(KEY_VALUE_SEPARATOR));
                Preconditions.checkArgument(pair.size() == 2, String.format("Incomplete key-value pair provided in the map field: %s", name));
                map.put(keyConverter.apply(pair.get(0)), valueConverter.apply(pair.get(1)));
            });
        }
        return map;
    }

    public static String convertStreamSegmentRecordToString(StreamSegmentRecord segmentRecord) {
        StringBuilder segmentStringBuilder = new StringBuilder();
        STREAM_SEGMENT_RECORD_FIELD_MAP.forEach((name, f) -> appendField(segmentStringBuilder, name, f.apply(segmentRecord), "|", "-"));
        return segmentStringBuilder.toString();
    }

    public static StreamSegmentRecord convertStringToStreamSegmentRecord(String segmentString) {
        Map<String, String> segmentDataMap = parseStringData(segmentString, "|", "-");
        return StreamSegmentRecord.newSegmentRecord(
                Integer.parseInt(getAndRemoveIfExists(segmentDataMap, STREAM_SEGMENT_RECORD_SEGMENT_NUMBER)),
                Integer.parseInt(getAndRemoveIfExists(segmentDataMap, STREAM_SEGMENT_RECORD_CREATION_EPOCH)),
                Long.parseLong(getAndRemoveIfExists(segmentDataMap, STREAM_SEGMENT_RECORD_CREATION_TIME)),
                Double.parseDouble(getAndRemoveIfExists(segmentDataMap, STREAM_SEGMENT_RECORD_KEY_START)),
                Double.parseDouble(getAndRemoveIfExists(segmentDataMap, STREAM_SEGMENT_RECORD_KEY_END)));
    }
}
