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
package io.pravega.controller.store.stream;

/**
 * Enumeration that encapsulates the result of comparison of two streamcuts.
 * Two streamcuts are compared by comparing each segment and offset in each of the streamcuts with the corresponding 
 * overlapping segments in the second streamcut. 
 * A segment X from Streamcut SC1 is considered being ahead of segment Y in streamcut SC2 if either X is a successor of Y OR
 * both X and Y are same segments and the offset in SC1 for X is greater than offset for X in SC2. 
 */
public enum StreamCutComparison {
    // Indicates that for each segment S in streamcut SC1 there exists either the same segment with same or lesser offset 
    // in streamcut SC2 OR all overlapping segments of S in streamcut SC2 are all predecessors of S. 
    EqualOrAfter,
    // Indicates that there are at least two segments S1 and S2 in streamcut SC1 such that:
    // S1 is ahead of some of the segments in SC2 that overlap with its keyrange. 
    // S2 is behind some of the segments in SC2 that overlap with its keyrange.  
    Overlaps,
    // Indicates that there are at least one segment S in streamcut SC1 such that:
    // S is behind its overlapping segments in SC2. And there are no segments S' in SC1 which are strictly ahead of corresponding  
    // overlapping segments in SC2.
    Before
}
