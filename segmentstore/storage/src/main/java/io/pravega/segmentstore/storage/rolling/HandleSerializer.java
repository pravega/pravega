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
package io.pravega.segmentstore.storage.rolling;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.pravega.common.io.ByteBufferOutputStream;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import java.nio.charset.Charset;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Map;
import java.util.StringTokenizer;
import lombok.val;

/**
 * Serializes and deserializes RollingSegmentHandles.
 */
final class HandleSerializer {
    //region Serialization Constants.

    private static final Charset ENCODING = Charsets.UTF_8;
    private static final String KEY_POLICY_MAX_SIZE = "maxsize";
    private static final String KEY_CONCAT = "concat";
    private static final String KEY_VALUE_SEPARATOR = "=";
    private static final String SEPARATOR = "&";
    private static final String CONCAT_SEPARATOR = "@";

    //endregion

    //region Serialization

    /**
     * Deserializes the given byte array into a RollingSegmentHandle.
     *
     * @param serialization The byte array to deserialize.
     * @param headerHandle  The SegmentHandle for the Header file.
     * @return A new instance of the RollingSegmentHandle class.
     */
    static RollingSegmentHandle deserialize(byte[] serialization, SegmentHandle headerHandle) {
        StringTokenizer st = new StringTokenizer(new String(serialization, ENCODING), SEPARATOR, false);
        Preconditions.checkArgument(st.hasMoreTokens(), "No separators in serialization.");

        SegmentRollingPolicy policy = null;
        OffsetAdjuster om = new OffsetAdjuster();
        long lastOffset = 0;
        ArrayList<SegmentChunk> segmentChunks = new ArrayList<>();
        while (st.hasMoreTokens()) {
            val entry = parse(st.nextToken());
            if (entry.getKey().equalsIgnoreCase(KEY_POLICY_MAX_SIZE)) {
                // Rolling policy entry: only check if we don't have it set yet.
                if (policy == null) {
                    Preconditions.checkArgument(isValidLong(entry.getValue()), "Invalid entry value for '%s'.", entry);
                    policy = new SegmentRollingPolicy(Long.parseLong(entry.getValue()));
                }
            } else if (entry.getKey().equalsIgnoreCase(KEY_CONCAT)) {
                // Concat entry header. This contains information about an upcoming concat.
                val concatInfo = parseConcat(entry.getValue());
                om.set(concatInfo.getKey(), concatInfo.getValue());
            } else {
                // Regular offset->file entry.
                Preconditions.checkArgument(isValidLong(entry.getKey()), "Invalid key value for '%s'.", entry);
                long offset = om.adjustOffset(Long.parseLong(entry.getKey()));
                SegmentChunk s = new SegmentChunk(entry.getValue(), offset);
                Preconditions.checkArgument(lastOffset <= s.getStartOffset(),
                        "SegmentChunk Entry '%s' has out-of-order offset (previous=%s).", s, lastOffset);
                segmentChunks.add(s);
                lastOffset = s.getStartOffset();
            }
        }

        RollingSegmentHandle h = new RollingSegmentHandle(headerHandle, policy, segmentChunks);
        h.setHeaderLength(serialization.length);
        return h;
    }

    /**
     * Serializes an entire RollingSegmentHandle into a new ByteArraySegment.
     *
     * @param handle The RollingSegmentHandle to serialize.
     * @return A ByteArraySegment with the serialization.
     */
    static ByteArraySegment serialize(RollingSegmentHandle handle) {
        try (ByteBufferOutputStream os = new ByteBufferOutputStream()) {
            //1. Policy Max Size.
            os.write(combine(KEY_POLICY_MAX_SIZE, Long.toString(handle.getRollingPolicy().getMaxLength())));
            //2. Chunks.
            handle.chunks().forEach(chunk -> os.write(serializeChunk(chunk)));
            return os.getData();
        }
    }

    /**
     * Serializes a single SegmentChunk.
     *
     * @param segmentChunk The SegmentChunk to serialize.
     * @return A byte array containing the serialization.
     */
    static byte[] serializeChunk(SegmentChunk segmentChunk) {
        return combine(Long.toString(segmentChunk.getStartOffset()), segmentChunk.getName());
    }

    /**
     * Serializes an entry that indicates a number of SegmentChunks are concatenated at a specified offset.
     *
     * @param chunkCount   The number of SegmentChunks to concat.
     * @param concatOffset The concat offset.
     * @return A byte array containing the serialization.
     */
    static byte[] serializeConcat(int chunkCount, long concatOffset) {
        return combine(KEY_CONCAT, chunkCount + CONCAT_SEPARATOR + concatOffset);
    }

    private static byte[] combine(String key, String value) {
        return (key + KEY_VALUE_SEPARATOR + value + SEPARATOR).getBytes(ENCODING);
    }

    private static Map.Entry<String, String> parse(String entry) {
        int sp = entry.indexOf(KEY_VALUE_SEPARATOR);
        Preconditions.checkArgument(sp > 0 && sp < entry.length() - 1, "Header entry '%s' is invalid.", entry);

        String key = entry.substring(0, sp);
        String value = entry.substring(sp + KEY_VALUE_SEPARATOR.length());
        Preconditions.checkArgument(!Strings.isNullOrEmpty(key), "Missing entry key for '%s'.", entry);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value), "Missing entry value for '%s'.", entry);
        return new AbstractMap.SimpleImmutableEntry<>(key, value);
    }

    private static Map.Entry<Long, Integer> parseConcat(String concat) {
        int pos = concat.indexOf(CONCAT_SEPARATOR);
        Preconditions.checkArgument(pos > 0 && pos < concat.length() - 1, "%s value '%s' is invalid.", KEY_CONCAT, concat);
        String count = concat.substring(0, pos);
        String offset = concat.substring(pos + CONCAT_SEPARATOR.length());
        try {
            val result = new AbstractMap.SimpleImmutableEntry<Long, Integer>(Long.parseLong(offset), Integer.parseInt(count));
            Preconditions.checkArgument(result.getKey() >= 0 && result.getValue() >= 0, "%s value '%s' is invalid.", KEY_CONCAT, concat);
            return result;
        } catch (NumberFormatException nfe) {
            throw new IllegalArgumentException(String.format("%s value '%s' is invalid.", KEY_CONCAT, concat), nfe);
        }
    }

    private static boolean isValidLong(String s) {
        try {
            Long.parseLong(s);
            return true;
        } catch (NumberFormatException nfe) {
            return false;
        }
    }

    ///endregion

    //region OffsetAdjuster

    /**
     * Helps adjust offsets when deserializing SegmentChunks from the Header.
     */
    private static class OffsetAdjuster {
        private long offsetAdjustment;
        private int remainingCount;
        private int originalCount;

        /**
         * Sets the given offset adjustment.
         *
         * @param offsetAdjustment The offset adjustment.
         * @param count            The number of times to apply the offset adjustment (counted by the number of calls to adjustOffset()).
         */
        void set(long offsetAdjustment, int count) {
            this.offsetAdjustment = offsetAdjustment;
            this.remainingCount = count;
            this.originalCount = count;
        }

        /**
         * Adjusts the given offset, if necessary. The offset will be adjusted by the amount specified by the last call
         * to set() if this method hasn't been called more than the specified number of times.
         *
         * @param offset The offset to adjust.
         * @return The adjusted offset.
         */
        long adjustOffset(long offset) {
            if (offset != 0 && this.remainingCount == this.originalCount) {
                // We haven't done any adjustments, yet we encountered an unexpected offset: reset.
                set(0, 0);
            } else if (this.remainingCount > 0) {
                this.remainingCount--;
                offset += this.offsetAdjustment;
            }

            return offset;
        }
    }

    //endregion
}
