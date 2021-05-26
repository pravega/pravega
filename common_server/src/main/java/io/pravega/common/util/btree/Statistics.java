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
package io.pravega.common.util.btree;

import com.google.common.base.Preconditions;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import java.io.IOException;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Statistics about a {@link BTreeIndex}.
 */
@Getter
@Builder
@EqualsAndHashCode
public final class Statistics {
    /**
     * Empty Statistics.
     */
    static final Statistics EMPTY = new Statistics(0, 0);
    static final Serializer SERIALIZER = new Serializer();
    /**
     * Number of Data Entries (only counting within Data Pages - does not include Index page pointers).
     */
    private final long entryCount;
    /**
     * Number of pages (Data + Entry).
     */
    private final long pageCount;

    /**
     * Creates a new {@link Statistics} object by applying the given deltas to the current {@link Statistics} object.
     *
     * @param entryCountDelta The delta to apply to {@link #getEntryCount()}.
     * @param pageCountDelta  The delta to apply to {@link #getPageCount()}.
     * @return A new {@link Statistics} object.
     */
    Statistics update(int entryCountDelta, int pageCountDelta) {
        long newEntryCount = this.entryCount + entryCountDelta;
        long newPageCount = this.pageCount + pageCountDelta;
        Preconditions.checkArgument(newEntryCount >= 0,
                "New Entry Count would be negative (EntryCount=%s, Delta=%s)", this.entryCount, entryCountDelta);
        Preconditions.checkArgument(newPageCount >= 0,
                "New Page Count would be negative (PageCount=%s, Delta=%s)", this.pageCount, pageCountDelta);
        return new Statistics(newEntryCount, newPageCount);
    }

    @Override
    public String toString() {
        return String.format("EntryCount = %s, PageCount = %s", this.entryCount, this.pageCount);
    }

    static class StatisticsBuilder implements ObjectBuilder<Statistics> {
    }

    static class Serializer extends VersionedSerializer.WithBuilder<Statistics, StatisticsBuilder> {
        @Override
        protected StatisticsBuilder newBuilder() {
            return builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(Statistics s, RevisionDataOutput target) throws IOException {
            target.writeCompactLong(s.entryCount);
            target.writeCompactLong(s.pageCount);
        }

        private void read00(RevisionDataInput source, StatisticsBuilder b) throws IOException {
            b.entryCount(source.readCompactLong());
            b.pageCount(source.readCompactLong());
        }
    }
}
