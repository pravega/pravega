namespace java com.emc.pravega.controller.stream.api

enum Status {
    SUCCESS,
    FAILURE,
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
  1: required string name,
  2: required ScalingPolicy policy
}

struct SegmentId {
  1: required string scope,
  2: required string name,
  3: required i32 number,
  4: required i32 previous
}

struct Position {
  1: required map<SegmentId, i64> ownedLogs,
  2: required map<SegmentId, i64> futureOwnedLogs
}

/*
 * Producer APIs supported by Stream Controller service
 */
service ProducerService {
    Status createStream (1: StreamConfig streamConfig)
    Status alterStream (1: StreamConfig streamConfig)
    list<string>  getCurrentSeqments(1:string stream)
    string getURL(1:SegmentId id)
}

/*
 * Consumer APIs supported by Stream Controller service
 */
service ConsumerService {
    list<Position> getPositions(1:string stream, 2:i64 timestamp, 3:i32 count)
    list<Position> updatePositions(1:list<Position> positions)
}

//TODO: Placeholder for Pravega Host to Stream Controller APIs.
