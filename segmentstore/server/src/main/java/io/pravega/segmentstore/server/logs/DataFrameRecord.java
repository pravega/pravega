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
package io.pravega.segmentstore.server.logs;

import io.pravega.segmentstore.contracts.SequencedElement;
import io.pravega.segmentstore.storage.LogAddress;
import java.util.List;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Singular;

/**
 * Represents a DataFrame Read Result, wrapping a LogItem.
 */
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public class DataFrameRecord<T extends SequencedElement> {
    /**
     * The wrapped Log Operation.
     */
    @Getter
    private final T item;

    private final RecordInfo recordInfo;

    /**
     * The Address of the Last Data Frame containing the LogItem. If the LogItem fits on exactly one DataFrame, this
     * will contain the Address for that Data Frame; if it spans multiple data frames, this stores the last data frame address.
     */
    LogAddress getLastUsedDataFrameAddress() {
        return this.recordInfo.getLastUsedDataFrameAddress();
    }

    /**
     * The Address of the Last Data Frame that ends with a part of the LogItem. If
     * the LogItem fits on exactly one DataFrame, this will return the Address for that Data Frame; if it spans
     * multiple data frames, it returns the Address of the last Data Frame that ends with a part of the LogItem
     * (in general, this is the Data Frame immediately preceding that returned by getLastUsedDataFrameAddress()).
     */
    LogAddress getLastFullDataFrameAddress() {
        return this.recordInfo.getLastFullDataFrameAddress();
    }

    /**
     * Whether the wrapped LogItem is the last entry in its Data Frame.
     */
    boolean isLastFrameEntry() {
        return this.recordInfo.isLastFrameEntry();
    }

    /**
     * An ordered list of EntryInfo objects representing metadata about the actual serialization of this DataFrameRecord item.
     */
    List<EntryInfo> getFrameEntries() {
        return this.recordInfo.getEntries();
    }

    @Override
    public String toString() {
        return String.format("%s, DataFrameSN = %d, LastInDataFrame = %s", getItem(), this.getLastUsedDataFrameAddress().getSequence(), isLastFrameEntry());
    }

    //region RecordInfo

    /**
     * Metadata about a particular DataFrameRecord.
     */
    @RequiredArgsConstructor(access = AccessLevel.PACKAGE)
    @Builder
    static class RecordInfo {
        /**
         * The Address of the Data Frame containing the last segment in this collection.
         * The return value of this method is irrelevant if hasData() == false.
         */
        @Getter
        private final LogAddress lastUsedDataFrameAddress;

        /**
         * The Address of the last Data Frame that ends with a segment in this collection.
         * If the number of segments is 1, then getLastFullDataFrameAddress() == getLastUsedDataFrameAddress().
         * The return value of this method is irrelevant if hasData() == false.
         */
        @Getter
        private final LogAddress lastFullDataFrameAddress;

        /**
         * Indicates whether the last segment in this collection is also the last entry in its Data Frame.
         * The return value of this method is irrelevant if hasData() == false.
         */
        @Getter
        private final boolean lastFrameEntry;

        @Getter
        @Singular
        private final List<DataFrameRecord.EntryInfo> entries;

        static class RecordInfoBuilder {
            LogAddress getLastUsedDataFrameAddress() {
                return this.lastUsedDataFrameAddress;
            }

            void withEntry(LogAddress frameAddress, int frameOffset, int length, boolean lastFrameEntry) {
                entry(new EntryInfo(frameAddress, frameOffset, length, lastFrameEntry));
            }
        }
    }

    //endregion

    /**
     * Metadata about a particular DataFrame Entry. A DataFrameRecord is made up of one or more DataFrame Entries.
     */
    @RequiredArgsConstructor(access = AccessLevel.PROTECTED)
    @Getter
    public static class EntryInfo {
        /**
         * Address of the containing DataFrame.
         */
        private final LogAddress frameAddress;
        /**
         * Offset within the DataFrame.
         */
        private final int frameOffset;
        /**
         * Contents length.
         */
        private final int length;
        /**
         * Whether it is the last entry in the DataFrame.
         */
        private final boolean lastEntryInDataFrame;
    }
}
