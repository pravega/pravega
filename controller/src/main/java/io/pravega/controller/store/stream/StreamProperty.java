package io.pravega.controller.store.stream;

import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
@Data
public class StreamProperty<T> implements Serializable{
    private final T property;
    private final boolean updating;

    public static <T> StreamProperty<T> startUpdate(final StreamProperty<T> current, final T update) {
        Preconditions.checkNotNull(current);
        Preconditions.checkNotNull(current.property);
        Preconditions.checkNotNull(update);
        Preconditions.checkArgument(!current.updating);

        return new StreamProperty<>(update, true);
    }

    public static <T> StreamProperty<T> complete(final StreamProperty<T> versionedProperty) {
        Preconditions.checkNotNull(versionedProperty);
        Preconditions.checkNotNull(versionedProperty.property);
        Preconditions.checkArgument(versionedProperty.updating);

        return new StreamProperty<>(versionedProperty.property, false);
    }
}
