package io.pravega.controller.store.stream;

import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class StreamProperty<T> implements Serializable{
    private final T property;
    private final boolean updating;

    public static <T> StreamProperty<T> update(final T update) {
        Preconditions.checkNotNull(update);

        return new StreamProperty<>(update, true);
    }

    public static <T> StreamProperty<T> complete(final T complete) {
        Preconditions.checkNotNull(complete);

        return new StreamProperty<>(complete, false);
    }
}
