package io.pravega.controller.store.stream.records;

import java.util.List;
import java.util.function.Function;

public class RecordHelper {
    public static <T> int binarySearch(final List<T> list, final int lower, final int upper, final long time, Function<T, Long> getTime) {
        if (upper < lower) {
            assert getTime.apply(list.get(0)) > time;
            // return index 0.
            return 0;
        }

        final int middle = (lower + upper) / 2;

        T middleRecord = list.get(middle);

        if (getTime.apply(middleRecord) <= time) {
            T next = list.size() > middle + 1 ? list.get(middle + 1) : null;
            if (next == null || (getTime.apply(next) > time)) {
                return middle;
            } else {
                return binarySearch(list, middle + 1, upper, time, getTime);
            }
        } else {
            return binarySearch(list, lower, middle - 1, time, getTime);
        }
    }

}
