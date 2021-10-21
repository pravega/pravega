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

import io.pravega.common.Exceptions;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * Parses Strings as a sequence of key-value pairs and allows extracting values for specific keys.
 * <p>
 * The input format is a sequence (delimited by {keyValueDelimiter}) of key-value pairs (separated by {pairDelimiter}):
 * "{key}{pairDelimiter}{value}{keyValueDelimiter}{key}{pairDelimiter}{value}{keyValueDelimiter}..."
 * <p>
 * The extractors are called on the {value} for the {key} that matches their own key. Call order of extractors is done
 * in the order in which the key-value pairs are parsed out of the string. In case of a malformed string, some extractors
 * may still be invoked until the error is discovered, at which point an exception is raised and no further extractors are invoked.
 * <p>
 * Example usage:
 * <pre>
 * {@code
 * AtomicInteger a = new AtomicInteger();
 * AtomicInteger b = new AtomicLong();
 * AtomicBoolean c = new AtomicBoolean();
 * AtomicReference<String> d = new AtomicReference<>();
 * DelimitedStringParser.parse(",", "=")
 *                      .extractInteger("a", a::set)
 *                      .extractLong("b", b::set)
 *                      .extractBoolean("c", c::set)
 *                      .extractString("d", d::set)
 *                      .parse("a=1,b=2,c=tRuE,d=foo");
 * System.out.println(String.format("a=%s, b=%s, c=%s, d=%s", a, b, c, d);
 * // Output: "a=1, b=2, c=true, d=foo"
 * }
 * </pre>
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class DelimitedStringParser {
    private final String pairDelimiter;
    private final String keyValueDelimiter;
    private final Map<String, Extractor<?>> extractors = new HashMap<>();

    //region Static Constructor

    /**
     * Creates a new DelimitedStringParser with the given Pair and KeyValue delimiters.
     *
     * @param pairDelimiter     A String that will be used to delimit pairs.
     * @param keyValueDelimiter A String that will be used to delimit Keys from Values (inside a pair).
     * @return A new instance of the DelimitedStringParser class.
     */
    public static DelimitedStringParser parser(String pairDelimiter, String keyValueDelimiter) {
        Exceptions.checkNotNullOrEmpty(pairDelimiter, "pairDelimiter");
        Exceptions.checkNotNullOrEmpty(keyValueDelimiter, "keyValueDelimiter");
        Preconditions.checkArgument(!pairDelimiter.equals(keyValueDelimiter),
                "pairDelimiter (%s) cannot be the same as keyValueDelimiter (%s)", pairDelimiter, keyValueDelimiter);
        return new DelimitedStringParser(pairDelimiter, keyValueDelimiter);
    }

    //endregion

    //region Configuration

    /**
     * Associates the given consumer with the given key. This consumer will be invoked every time a Key-Value pair with
     * the given key is encountered (argument is the Value of the pair). Note that this may be invoked multiple times or
     * not at all, based on the given input.
     *
     * @param key      The key for which to invoke the consumer.
     * @param consumer The consumer to invoke.
     * @return This object instance.
     */
    public DelimitedStringParser extractInteger(String key, Consumer<Integer> consumer) {
        this.extractors.put(key, new Extractor<>(consumer, Integer::parseInt));
        return this;
    }

    /**
     * Associates the given consumer with the given key. This consumer will be invoked every time a Key-Value pair with
     * the given key is encountered (argument is the Value of the pair). Note that this may be invoked multiple times or
     * not at all, based on the given input.
     *
     * @param key      The key for which to invoke the consumer.
     * @param consumer The consumer to invoke.
     * @return This object instance.
     */
    public DelimitedStringParser extractLong(String key, Consumer<Long> consumer) {
        this.extractors.put(key, new Extractor<>(consumer, Long::parseLong));
        return this;
    }

    /**
     * Associates the given consumer with the given key. This consumer will be invoked every time a Key-Value pair with
     * the given key is encountered (argument is the Value of the pair). Note that this may be invoked multiple times or
     * not at all, based on the given input.
     *
     * @param key      The key for which to invoke the consumer.
     * @param consumer The consumer to invoke.
     * @return This object instance.
     */
    public DelimitedStringParser extractString(String key, Consumer<String> consumer) {
        this.extractors.put(key, new Extractor<>(consumer, s -> s));
        return this;
    }

    /**
     * Associates the given consumer with the given key. This consumer will be invoked every time a Key-Value pair with
     * the given key is encountered (argument is the Value of the pair). Note that this may be invoked multiple times or
     * not at all, based on the given input.
     *
     * @param key      The key for which to invoke the consumer.
     * @param consumer The consumer to invoke.
     * @return This object instance.
     */
    public DelimitedStringParser extractBoolean(String key, Consumer<Boolean> consumer) {
        this.extractors.put(key, new Extractor<>(consumer,
                value -> {
                    value = value.trim().toLowerCase();
                    return value.equals("true") || value.equals("yes") || value.equals("1");
                }));
        return this;
    }

    /**
     * Parses the given string using the configuration set on this parser.
     *
     * @param s The string to parse.
     */
    public void parse(String s) {
        Preconditions.checkNotNull(s, "s");
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

            Extractor<?> e = this.extractors.get(key);
            Preconditions.checkArgument(e != null, String.format("No extractor provided for key '%s'.", key));
            e.process(value);
        }
    }

    //region Extractors

    @RequiredArgsConstructor
    private static class Extractor<T> {
        private final Consumer<T> callback;
        private final Function<String, T> converter;

        private void process(String value) {
            this.callback.accept(this.converter.apply(value));
        }
    }

    //endregion
}
