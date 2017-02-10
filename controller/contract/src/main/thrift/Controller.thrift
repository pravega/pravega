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
 * Writer, Reader and Admin APIs supported by Stream Controller Service
 */
service ControllerService {
    CreateStreamStatus createStream (1: StreamConfig streamConfig)
    UpdateStreamStatus alterStream (1: StreamConfig streamConfig)
    UpdateStreamStatus sealStream(1: string scope, 2:string stream)
    list<SegmentRange> getCurrentSegments(1:string scope, 2:string stream)
    list<Position> getPositions(1:string scope, 2:string stream, 3:i64 timestamp, 4:i32 count)
    map<SegmentId,list<i32>> getSegmentsImmediatlyFollowing(1:SegmentId segment)
    ScaleResponse scale(1:string scope, 2:string stream, 3:list<i32> sealedSegments, 4:map<double, double> newKeyRanges, 5:i64 scaleTimestamp)
    NodeUri getURI(1: SegmentId segment)
    bool isSegmentValid(1: string scope, 2: string stream, 3: i32 segmentNumber)
    TxnId createTransaction(1:string scope, 2:string stream)
    TxnStatus commitTransaction(1:string scope, 2:string stream, 3:TxnId txnid)
    TxnStatus abortTransaction(1:string scope, 2:string stream, 3:TxnId txnid)
    TxnState  checkTransactionStatus(1:string scope, 2:string stream, 3:TxnId txnid)
}
//TODO: Placeholder for Pravega Host to Stream Controller APIs.
