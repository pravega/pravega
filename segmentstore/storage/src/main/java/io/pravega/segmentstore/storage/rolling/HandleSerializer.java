package io.pravega.segmentstore.storage.rolling;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.pravega.segmentstore.storage.SegmentHandle;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.function.Predicate;
import lombok.val;

final class HandleSerializer {
    private static final String KEY_NAME = "name";
    private static final String KEY_POLICY_MAX_SIZE = "maxsize";
    private static final String KEY_VALUE_SEPARATOR = "=";
    private static final String SEPARATOR = "&";

    static RollingStorageHandle deserialize(String serialization, SegmentHandle headerHandle) {
        StringTokenizer st = new StringTokenizer(serialization, SEPARATOR, false);
        Preconditions.checkArgument(st.hasMoreTokens(), "No separators in serialization.");

        // 1. Segment Name.
        val nameEntry = parse(st.nextToken(), KEY_NAME::equals, HandleSerializer::nonEmpty);
        // 2. Policy Max Size.
        val policyEntry = parse(st.nextToken(), KEY_POLICY_MAX_SIZE::equals, HandleSerializer::isValidLong);
        // 3. SubSegments.
        ArrayList<RollingStorageHandle.SubSegment> subSegments = new ArrayList<>();
        long lastOffset = -1;
        while (st.hasMoreTokens()) {
            val entry = parse(st.nextToken(), HandleSerializer::isValidLong, HandleSerializer::nonEmpty);
            RollingStorageHandle.SubSegment s = parse(entry);
            Preconditions.checkArgument(lastOffset < s.getStartOffset(), "SubSegment Entry '%s' has out-of-order offset (previous=%s).", s, lastOffset);
            subSegments.add(s);
            lastOffset = s.getStartOffset();
        }

        return new RollingStorageHandle(nameEntry.getValue(), headerHandle, new RollingPolicy(Long.parseLong(policyEntry.getValue())), subSegments);
    }

    /**
     * Serializes a single SubSegment into a String
     */
    static String serialize(RollingStorageHandle.SubSegment subSegment) {
        return combine(Long.toString(subSegment.getStartOffset()), subSegment.getName());
    }

    /**
     * Serializes an entire RollingStorageHandle into a new ByteArraySegment.
     *
     * @param handle
     * @return
     */
    static String serialize(RollingStorageHandle handle) {
        StringBuilder sb = new StringBuilder();
        //1. Segment Name.
        sb.append(combine(KEY_NAME, handle.getSegmentName()));
        //2. Policy Max Size.
        sb.append(combine(KEY_POLICY_MAX_SIZE, Long.toString(handle.getRollingPolicy().getMaxLength())));
        //3. SubSegments.
        handle.forEachSubSegment(s -> sb.append(serialize(s)));
        return sb.toString();
    }

    private static String combine(String key, String value) {
        return key + KEY_VALUE_SEPARATOR + value + SEPARATOR;
    }

    private static Map.Entry<String, String> parse(String entry, Predicate<String> keyValidator, Predicate<String> valueValidator) {
        int sp = entry.indexOf(KEY_VALUE_SEPARATOR);
        Preconditions.checkArgument(sp > 0 && sp < entry.length() - 1, "Header entry '%s' is invalid.", entry);

        String key = entry.substring(0, sp);
        Preconditions.checkArgument(keyValidator.test(key), "Invalid entry key for '%s'.", entry);

        String value = entry.substring(sp + 1);
        Preconditions.checkArgument(valueValidator.test(key), "Invalid entry value for '%s'.", entry);
        return new AbstractMap.SimpleImmutableEntry<>(key, value);
    }

    private static RollingStorageHandle.SubSegment parse(Map.Entry<String, String> entry) {
        return new RollingStorageHandle.SubSegment(entry.getValue(), Long.parseLong(entry.getKey()));
    }

    private static boolean nonEmpty(String s) {
        return !Strings.isNullOrEmpty(s);
    }

    private static boolean isValidLong(String s) {
        try {
            Long.parseLong(s);
            return true;
        } catch (NumberFormatException nfe) {
            return false;
        }
    }
}
