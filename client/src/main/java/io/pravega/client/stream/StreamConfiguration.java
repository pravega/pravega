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
package io.pravega.client.stream;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Singular;

/**
 * The configuration of a Stream.
 */
@Data
@Builder(toBuilder = true)
public class StreamConfiguration implements Serializable {

    private static final long serialVersionUID = 1L;
    /*
       Maximum number of Tags per Stream is 128.
     */
    private static final int MAX_TAG_COUNT = 128;
    /*
       Maximum length of each Tag is 256 characters.
     */
    private static final int MAX_TAG_LENGTH = 256;

    /**
     * API to return scaling policy.
     *
     * @param scalingPolicy The Stream Scaling policy.
     * @return Scaling Policy for the Stream.
     */
    private final ScalingPolicy scalingPolicy;

    /**
     * API to return retention policy.
     * Also see: {@link ReaderGroupConfig.StreamDataRetention}
     * @param retentionPolicy The Stream Retention policy.
     * @return Retention Policy for the Stream.
     */
    private final RetentionPolicy retentionPolicy;
    
    /**
     * The duration after the last call to {@link EventStreamWriter#noteTime(long)} which the
     * timestamp should be considered valid before it is forgotten. Meaning that after this long of
     * not calling {@link EventStreamWriter#noteTime(long)} the writer will be forgotten.
     * If there are no known writers, readers that call {@link EventStreamReader#getCurrentTimeWindow(Stream)}
     * will receive a `null` when they are at the corresponding position in the stream.
     *
     * @param timestampAggregationTimeout The duration after the last call to {@link EventStreamWriter#noteTime(long)}
     *                                    which the timestamp should be considered valid before it is forgotten.
     * @return The duration after the last call to {@link EventStreamWriter#noteTime(long)} which the timestamp should
     * be considered valid before it is forgotten.
     */
    private final long timestampAggregationTimeout;

    /**
     * API to return the configured tags for the Stream.
     * @param tag The tag(s) for the Stream.
     * @return List of tag(s) for the Stream.
     */
    @Singular
    @EqualsAndHashCode.Exclude
    private final Set<String> tags;

    public static final class StreamConfigurationBuilder {
        private ScalingPolicy scalingPolicy = ScalingPolicy.fixed(1);

        public StreamConfiguration build() {
            Set<String> tagSet = validateTags(this.tags);
            return new StreamConfiguration(this.scalingPolicy, this.retentionPolicy, this.timestampAggregationTimeout, tagSet);
        }

        private Set<String> validateTags(List<String> tags) {
            Set<String> tagsSet;
            if (tags != null) {
                Preconditions.checkArgument(tags.size() < MAX_TAG_COUNT, "Maximum number of tags allowed is 128");
                tags.forEach(tag -> Preconditions.checkArgument(tag.length() < MAX_TAG_LENGTH, "Maximum length of a tag allowed is 256"));
            }
            switch (tags == null ? 0 : tags.size()) {
                case 0:
                    tagsSet = Collections.emptySet();
                    break;
                case 1:
                    tagsSet = Collections.singleton(this.tags.get(0));
                    break;
                default:
                    tagsSet = java.util.Collections.unmodifiableSet(new HashSet<>(this.tags));
            }
            return tagsSet;
        }
    }

    /**
     * Check if only the tags have been modified between the two StreamConfigurations.
     * @param cfg1 StreamConfiguration.
     * @param cfg2 StreamConfiguration.
     * @return boolean indicating if it is a tag only change.
     */
    public static boolean isTagOnlyChange(StreamConfiguration cfg1, StreamConfiguration cfg2) {
        return cfg1.equals(cfg2) && !cfg1.tags.equals(cfg2.tags);
    }
}
