/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.common.util;

import com.emc.pravega.common.Exceptions;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * Utility methods that help with various String-related actions.
 */
public final class StringUtils {
    /**
     * Parses the given string as a sequence of key-value pairs and applies the given extractors on the values for which
     * their keys match.
     * <p>
     * The input format is a sequence (delimited by {keyValueDelimiter}) of key-value pairs (separated by {pairDelimiter}):
     * "{key}{pairDelimiter}{value}{keyValueDelimiter}{key}{pairDelimiter}{value}{keyValueDelimiter}..."
     * <p>
     * The extractors are called on the {value} for the {key} that matches their own key. Call order of extractors is done
     * in the order in which the key-value pairs are parsed out of the string. In case of a malformed string, some extractors
     * may still be invoked until the error is discovered, at which point an exception is raised and no further extractors are invoked.
     *
     * @param s                 The string to parse.
     * @param pairDelimiter     A String that separates different key-value pairs.
     * @param keyValueDelimiter A String that separates keys from values.
     * @param extractors        A collection of Extractors that should be applied.
     */
    public static void parse(String s, String pairDelimiter, String keyValueDelimiter, Extractor... extractors) {
        Preconditions.checkArgument(extractors != null && extractors.length > 0, "extractors");
        val indexedExtractors = Arrays.stream(extractors).collect(Collectors.toMap(Extractor::getKey, e -> e));
        parsePairs(s,
                pairDelimiter,
                keyValueDelimiter,
                (key, value) -> {
                    Extractor e = indexedExtractors.get(key);
                    Preconditions.checkArgument(e != null, String.format("No extractor provided for key '%s'.", key));
                    e.process(value);
                });
    }

    /**
     * Parses the given string as a sequence of key-value pairs and invokes the given callback on each such pair.
     * <p>
     * The input format is a sequence (delimited by {keyValueDelimiter}) of key-value pairs (separated by {pairDelimiter}):
     * "{key}{pairDelimiter}{value}{keyValueDelimiter}{key}{pairDelimiter}{value}{keyValueDelimiter}..."
     * <p>
     * Each key-value pair is parsed in the order in which it exist in the string, which is the same order in which the
     * callback is invoked. In case of a malformed string, the callback will be invoked until the error is discovered, at
     * which point an exception is raised and no further callback invocations are made.
     *
     * @param s                 The string to parse.
     * @param pairDelimiter     A String that separates different key-value pairs.
     * @param keyValueDelimiter A String that separates keys from values.
     * @param callback          A Callback that will be invoked for each key-value pair. The first argument is the key, the second
     *                          argument is the value.
     */
    static void parsePairs(String s, String pairDelimiter, String keyValueDelimiter, BiConsumer<String, String> callback) {
        Preconditions.checkNotNull(s, "s");
        Exceptions.checkNotNullOrEmpty(pairDelimiter, "pairDelimiter");
        Exceptions.checkNotNullOrEmpty(keyValueDelimiter, "keyValueDelimiter");
        Preconditions.checkArgument(!pairDelimiter.equals(keyValueDelimiter),
                "pairDelimiter (%s) cannot be the same as keyValueDelimiter (%s)", pairDelimiter, keyValueDelimiter);
        Preconditions.checkNotNull(callback, "callback");
        if (s.length() == 0) {
            // Nothing to do.
            return;
        }

        val pairs = s.split(pairDelimiter);
        for (String pair : pairs) {
            int delimiterPos = pair.indexOf(keyValueDelimiter);
            if (delimiterPos < 0) {
                throw new IllegalArgumentException(String.format("Invalid pair '%s' (missing key-value delimiter).", pair));
            }

            String key = pair.substring(0, delimiterPos);
            String value;
            if (delimiterPos == pair.length() - 1) {
                value = "";
            } else {
                value = pair.substring(delimiterPos + 1);
            }

            callback.accept(key, value);
        }
    }

    //region Extractors

    /**
     * Defines a base Extractor.
     *
     * @param <T> Type of the argument.
     */
    @RequiredArgsConstructor
    public static abstract class Extractor<T> {
        @Getter
        private final String key;
        private final Consumer<T> callback;
        private final Function<String, T> converter;

        private void process(String value) {
            this.callback.accept(this.converter.apply(value));
        }
    }

    /**
     * An Extractor that interprets the values as Integers.
     */
    public static final class Int32Extractor extends Extractor<Integer> {
        public Int32Extractor(String key, Consumer<Integer> callback) {
            super(key, callback, Integer::parseInt);
        }
    }

    /**
     * An Extractor that interprets the values as Longs.
     */
    public static final class Int64Extractor extends Extractor<Long> {
        public Int64Extractor(String key, Consumer<Long> callback) {
            super(key, callback, Long::parseLong);
        }
    }

    /**
     * An Extractor that interprets the values as Strings.
     */
    public static final class StringExtractor extends Extractor<String> {
        public StringExtractor(String key, Consumer<String> callback) {
            super(key, callback, s -> s);
        }
    }

    /**
     * An Extractor that interprets the values as Booleans (case-insensitive).
     * Values for 'true': "true", "yes", "1". All other values are interpreted as 'false'. This is an expanded version
     * of Boolean.parseString (which interprets "true" as true, all else is false).
     */
    public static final class BooleanExtractor extends Extractor<Boolean> {
        public BooleanExtractor(String key, Consumer<Boolean> callback) {
            super(key, callback, BooleanExtractor::parse);
        }

        private static Boolean parse(String value) {
            value = value.trim().toLowerCase();
            return value.equals("true") || value.equals("yes") || value.equals("1");
        }
    }

    //endregion
}
