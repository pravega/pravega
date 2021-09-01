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

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class StreamConfigurationTest {

    @Test
    public void testStreamConfigDefault() {
        StreamConfiguration streamConfig = StreamConfiguration.builder().build();
        assertEquals(ScalingPolicy.fixed(1), streamConfig.getScalingPolicy() );
        assertEquals(0, streamConfig.getTags().size());
        assertEquals(0L, streamConfig.getRolloverSizeBytes());
    }

    @Test
    public void testStreamBuilder() {
        StreamConfiguration streamConfig = StreamConfiguration.builder().tag("tag1").build();
        assertEquals(Collections.singleton("tag1"), streamConfig.getTags());
        Set<String> tagList = ImmutableSet.of("tag1", "tag2");
        streamConfig = StreamConfiguration.builder().tag("tag1").tag("tag2").build();
        assertEquals(ScalingPolicy.fixed(1), streamConfig.getScalingPolicy() );
        assertEquals(2, streamConfig.getTags().size());
        assertEquals(tagList, streamConfig.getTags());

        streamConfig = StreamConfiguration.builder().tags(tagList).rolloverSizeBytes(1024).build();
        assertEquals(2, streamConfig.getTags().size());
        assertEquals(tagList, streamConfig.getTags());
        assertEquals(1024L, streamConfig.getRolloverSizeBytes());
    }

    @Test
    public void testInvalidStreamConfig() {
        // validate duplicate tags are removed.
        StreamConfiguration cfg = StreamConfiguration.builder().tag("t1").tag("t1").build();
        assertEquals(1, cfg.getTags().size());
        // Invalid tag with length greater than 256
        String s = new String(new char[257]);
        assertThrows(IllegalArgumentException.class, () -> StreamConfiguration.builder().tag(s).build());
        // Exceed the permissible number of tags for String.
        List<String> tags = IntStream.range(0, 130).mapToObj(String::valueOf).collect(Collectors.toList());
        assertThrows(IllegalArgumentException.class, () -> StreamConfiguration.builder().tags(tags).build());
        // Invalid rollover size
        assertThrows(IllegalArgumentException.class, () -> StreamConfiguration.builder().rolloverSizeBytes(-1024).build());
    }

    @Test
    public void testCompareStreamConfig() {
        StreamConfiguration cfg1 = StreamConfiguration.builder().tag("t1").tag("t1").build();
        StreamConfiguration cfg2 = StreamConfiguration.builder().build();
        StreamConfiguration cfg3 = StreamConfiguration.builder().retentionPolicy(RetentionPolicy.bySizeBytes(100)).build();
        StreamConfiguration cfg4 = StreamConfiguration.builder().tag("t2").tag("t3").build();
        assertEquals(cfg1, cfg2);
        assertTrue(StreamConfiguration.isTagOnlyChange(cfg1, cfg2));
        assertNotEquals(cfg2, cfg3);
        assertFalse(StreamConfiguration.isTagOnlyChange(cfg1, cfg3));
        assertFalse(StreamConfiguration.isTagOnlyChange(cfg2, cfg3));
        assertTrue(StreamConfiguration.isTagOnlyChange(cfg1, cfg4));
    }
}