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
package io.pravega.shared.controller.tracing;

/**
 * RPC tracing tags.
 */
public final class RPCTracingTags {
    public static final String LIST_SCOPES = "listScopes";
    public static final String CREATE_SCOPE = "createScope";
    public static final String CHECK_SCOPE_EXISTS = "checkScopeExists";
    public static final String LIST_STREAMS_IN_SCOPE = "listStreamsInScope";
    public static final String LIST_STREAMS_IN_SCOPE_FOR_TAG = "listStreamsInScopeForTag";
    public static final String DELETE_SCOPE = "deleteScope";
    public static final String DELETE_SCOPE_RECURSIVE = "deleteScopeRecursive";
    public static final String CREATE_STREAM = "createStream";
    public static final String CHECK_STREAM_EXISTS = "checkStreamExists";
    public static final String GET_STREAM_CONFIGURATION = "getStreamConfiguration";
    public static final String UPDATE_STREAM = "updateStream";
    public static final String LIST_SUBSCRIBERS = "listSubscribers";
    public static final String UPDATE_TRUNCATION_STREAM_CUT = "updateTruncationStreamCut";
    public static final String TRUNCATE_STREAM = "truncateStream";
    public static final String SCALE_STREAM = "scaleStream";
    public static final String START_SCALE_STREAM = "scaleStream";
    public static final String CHECK_SCALE = "checkScale";
    public static final String SEAL_STREAM = "sealStream";
    public static final String DELETE_STREAM = "deleteStream";
    public static final String GET_SEGMENTS = "getSegmentsAtTime";
    public static final String GET_SEGMENTS_IMMEDIATELY_FOLLOWING = "getSegmentsImmediatelyFollowing";
    public static final String GET_CURRENT_SEGMENTS = "getCurrentSegments";
    public static final String GET_EPOCH_SEGMENTS = "getEpochSegments";
    public static final String GET_URI = "getURI";
    public static final String CREATE_TRANSACTION = "createTransaction";
    public static final String PING_TRANSACTION = "pingTransaction";
    public static final String CHECK_TRANSACTION_STATE = "checkTransactionState";
    public static final String LIST_COMPLETED_TRANSACTIONS = "listCompletedTransactions";
    public static final String NOTE_TIMESTAMP_FROM_WRITER = "noteTimestampFromWriter";
    public static final String REMOVE_WRITER = "removeWriter";
    public static final String CREATE_KEY_VALUE_TABLE = "createKeyValueTable";
    public static final String LIST_KEY_VALUE_TABLES = "listKeyValueTables";
    public static final String GET_KEY_VALUE_TABLE_CONFIGURATION = "getKeyValueTableConfiguration";
    public static final String DELETE_KEY_VALUE_TABLE = "deleteKeyValueTable";
    public static final String CREATE_READER_GROUP = "createReaderGroup";
    public static final String UPDATE_READER_GROUP = "updateReaderGroup";
    public static final String GET_READER_GROUP_CONFIG = "getReaderGroupConfig";
    public static final String DELETE_READER_GROUP = "deleteReaderGroup";
    public static final String GET_SUCCESSORS_FROM_CUT = "getSuccessorsFromCut";
    public static final String IS_SEGMENT_OPEN = "isSegmentOpen";
    public static final String COMMIT_TRANSACTION = "commitTransaction";
    public static final String ABORT_TRANSACTION = "abortTransaction";
    public static final String GET_OR_REFRESH_DELEGATION_TOKEN_FOR = "getOrRefreshDelegationTokenFor";
    public static final String GET_CURRENT_SEGMENTS_KEY_VALUE_TABLE = "getCurrentSegmentsKeyValueTable";
    public static final String GET_SEGMENTS_BETWEEN_STREAM_CUTS = "getSegmentsBetweenStreamCuts";
    public static final String IS_STREAMCUT_VALID = "isStreamCutValid";
    public static final String GET_CONTROLLER_TO_BUCKET_MAPPING = "getControllerToBucketMapping";
}
