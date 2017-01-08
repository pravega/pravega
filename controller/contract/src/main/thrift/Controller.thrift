namespace java com.emc.pravega.controller.stream.api.v1

enum CreateStreamStatus {
    SUCCESS,
    FAILURE,
    STREAM_EXISTS
}

enum UpdateStreamStatus {
    SUCCESS,
    FAILURE,
    STREAM_NOT_FOUND
}

enum ScaleStreamStatus {
    SUCCESS,
    FAILURE,
    PRECONDITION_FAILED,
    UPDATE_CONFLICT
}

enum TxnStatus {
    SUCCESS,
    FAILURE,
    STREAM_NOT_FOUND,
    TRANSACTION_NOT_FOUND
}

enum TxnState {
	UNKNOWN,
    OPEN,
    SEALED,
    COMMITTED,
    ABORTED
}

enum ScalingPolicyType {
    FIXED_NUM_SEGMENTS,
    BY_RATE_IN_BYTES,
    BY_RATE_IN_EVENTS,
}

struct ScalingPolicy {
  1: required ScalingPolicyType type,
  2: required i64 targetRate,
  3: required i32 scaleFactor,
  4: required i32 minNumSegments
}


struct StreamConfig {
  1: required string scope,
  2: required string name,
  3: required ScalingPolicy policy
}

struct SegmentId {
  1: required string scope,
  2: required string streamName,
  3: required i32 number
}

struct FutureSegment {
  1: required SegmentId futureSegment,
  2: required SegmentId precedingSegment
}

struct SegmentRange {
  1: required SegmentId segmentId,
  2: required double minKey,
  3: required double maxKey
}

struct NodeUri {
  1: required string endpoint;
  2: required i32 port;
}

struct Position {
  1: required map<SegmentId, i64> ownedSegments,
  2: required map<FutureSegment, i64> futureOwnedSegments
}

struct TxnId {
 1: required i64 highBits,
 2: required i64 lowBits
}

struct ScaleResponse {
 1: required ScaleStreamStatus status,
 2: required list<SegmentRange> segments,
}

/*
 * Producer, Consumer and Admin APIs supported by Stream Controller Service
 */
service ControllerService {
    CreateStreamStatus createStream (1: StreamConfig streamConfig)
    UpdateStreamStatus alterStream (1: StreamConfig streamConfig)
    list<SegmentRange> getCurrentSegments(1:string scope, 2:string stream)
    list<Position> getPositions(1:string scope, 2:string stream, 3:i64 timestamp, 4:i32 count)
    list<Position> updatePositions(1:string scope, 2:string stream, 3:list<Position> positions)
    ScaleResponse scale(1:string scope, 2:string stream, 3:list<i32> sealedSegments, 4:map<double, double> newKeyRanges, 5:i64 scaleTimestamp)
    NodeUri getURI(1: SegmentId segment)
    bool isSegmentValid(1: string scope, 2: string stream, 3: i32 segmentNumber)
    TxId createTransaction(1:string scope, 2:string stream)
    TransactionStatus commitTransaction(1:string scope, 2:string stream, 3:TxId txid)
    TransactionStatus dropTransaction(1:string scope, 2:string stream, 3:TxId txid)
    TxState checkTransactionStatus(1:string scope, 2:string stream, 3:TxId txid)
}
//TODO: Placeholder for Pravega Host to Stream Controller APIs.
