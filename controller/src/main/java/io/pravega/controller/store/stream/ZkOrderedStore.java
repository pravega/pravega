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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.TagLogger;
import io.pravega.controller.store.ZKStoreHelper;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static io.pravega.controller.store.stream.ZKStreamMetadataStore.DATA_NOT_FOUND_PREDICATE;

/**
 * This class is used to store ordered objects on a per stream basis. 
 * It uses Persistent Sequential znodes to create ordered collection of entries.
 * New entities can be added and existing entities can be removed from these ordered collections. 
 * All entities from the collection can be viewed and ordered based on the given order (a long). 
 * 
 * Since we use Persistent Sequential znodes to assign order, we can exhaust the order space which is basically 10 digits
 * (Integer.Max). So this store creates multiple ordered collection of collections and is responsible for internally 
 * creating and progressing new collections once a previous collection is exhausted. 
 * Overall we can create Integer.Max new collections. This means overall we have approximately Long.Max entries that 
 * each ordered collection can support in its lifetime. 
 */
class ZkOrderedStore {
    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(ZkOrderedStore.class));
    private static final String COLLECTIONS_NODE = "collections";
    private static final String SEALED_NODE = "sealed";
    private static final String ENTITIES_NODE = "entities";
    private static final String POSITION_NODE = "pos";
    private final ZKStoreHelper storeHelper;
    private final String ordererName;
    private final Executor executor;
    private final int rollOverAfter;

    /**
     * This class creates a collection of collections starting with collection numbered 0.
     * A new collection is created under the znode: 
     * \<root\>/scope/stream/collections\<collectionNum\>/entities
     * It adds entries to lowest numbered collection using a persistent sequential znode 
     * until its capacity is exhausted. 
     * Once a collection is exhausted, a successor collection with number `current + 1` is created and current successor 
     * is marked as sealed by created following znode: 
     *     `\<root\>/scope/stream/collections\<collectionNum\>/sealed
     * @param ordererName name of ordered which is used to generate the `root` of the orderer
     * @param storeHelper store helper
     * @param executor executor
     */
    ZkOrderedStore(String ordererName, ZKStoreHelper storeHelper, Executor executor) {
        // default roll over is after 90% or 1.8 billion entries have been added to a collection. 
        this(ordererName, storeHelper, executor, (Integer.MAX_VALUE / 10) * 9);
    }

    @VisibleForTesting
    ZkOrderedStore(String ordererName, ZKStoreHelper storeHelper, Executor executor, int rollOverAfter) {
        this.ordererName = ordererName;
        this.storeHelper = storeHelper;
        this.executor = executor;
        this.rollOverAfter = rollOverAfter;
    }

    /**
     * Method to add new entity to the ordered collection. Note: Same entity could be added to the collection multiple times.
     * Entities are added to the latest collection for the stream. If the collection has exhausted positions allowed for by rollOver limit,
     * a new successor collection is initiated and the entry is added to the new collection.
     * Any entries with positions higher than rollover values under a set are ignored and eventually purged. 
     * 
     * @param scope scope
     * @param stream stream 
     * @param entity entity to add
     * @param requestId
     * @return CompletableFuture which when completed returns the position where the entity is added to the set. 
     */
    CompletableFuture<Long> addEntity(String scope, String stream, String entity, long requestId) {
        // add persistent sequential node to the latest collection number 
        // if collectionNum is sealed, increment collection number and write the entity there. 
        return getLatestCollection(scope, stream)
                .thenCompose(latestcollectionNum -> {
                    return storeHelper.createPersistentSequentialZNode(getEntitySequentialPath(scope, stream, latestcollectionNum), entity.getBytes(Charsets.UTF_8))
                        .thenCompose(positionPath -> {
                            int position = getPositionFromPath(positionPath);
                            if (position > rollOverAfter) {
                                // if newly created position exceeds rollover limit, we need to delete that entry 
                                // and roll over. 
                                // 1. delete newly created path
                                return storeHelper.deletePath(positionPath, false)
                                                  // 2. seal latest collection
                                                  .thenCompose(v -> storeHelper.createZNodeIfNotExist(
                                                          getCollectionSealedPath(scope, stream, latestcollectionNum)))
                                                  // 3. create new collection and put the entity in
                                                  .thenCompose(v -> {
                                                      String path = getEntitySequentialPath(scope, stream, latestcollectionNum + 1);
                                                      return storeHelper.createPersistentSequentialZNode(path, entity.getBytes(Charsets.UTF_8));
                                                  })
                                                  .thenApply(newPositionPath -> Position.toLong(latestcollectionNum + 1, getPositionFromPath(newPositionPath)))
                                                  // 4. delete empty sealed collection path
                                                  .thenCompose(orderedPosition -> {
                                                      return tryDeleteSealedCollection(scope, stream, latestcollectionNum).thenApply(v -> orderedPosition);
                                                  });
                            } else {
                                return CompletableFuture.completedFuture(Position.toLong(latestcollectionNum, position));
                            }
                        });
                    })
                .whenComplete((r, e) -> {
                    if (e != null) {
                        log.error(requestId, 
                                "error encountered while trying to add entity {} for stream {}/{}", entity, scope, stream, e);
                    } else {
                        log.debug(requestId, 
                                "entity {} added for stream {}/{} at position {}", entity, scope, stream, r);
                    }
                });
    }

    /**
     * Method to remove entities from the ordered set. Entities are referred to by their position pointer.  
     * @param scope scope
     * @param stream stream
     * @param entities list of entities' positions to remove
     * @return CompletableFuture which when completed will indicate that entities are removed from the set. 
     */
    CompletableFuture<Void> removeEntities(String scope, String stream, Collection<Long> entities) {
        Set<Integer> collections = entities.stream().collect(Collectors.groupingBy(x -> new Position(x).collectionNumber)).keySet();
        return Futures.allOf(entities.stream()
                                     .map(entity -> storeHelper.deletePath(getEntityPath(scope, stream, entity), false))
                                     .collect(Collectors.toList()))
                      .thenCompose(v -> Futures.allOf(
                              collections.stream().map(collectionNum -> isSealed(scope, stream, collectionNum)
                                      .thenCompose(sealed -> {
                                          if (sealed) {
                                              return tryDeleteSealedCollection(scope, stream, collectionNum);
                                          } else {
                                              return CompletableFuture.completedFuture(null);
                                          }
                                      })).collect(Collectors.toList())))
                      .whenComplete((r, e) -> {
                          if (e != null) {
                              log.error("error encountered while trying to remove entity positions {} for stream {}/{}", entities, scope, stream, e);
                          } else {
                              log.debug("entities at positions {} removed for stream {}/{}", entities, scope, stream);
                          }
                      });
    }

    /**
     * Returns a map of position to entity that was added to the set. 
     * Note: Entities are ordered by position in the set but the map responded from this api is not ordered by default.
     * Users can filter and order elements based on the position and entity id.  
     * @param scope scope scope
     * @param stream stream stream
     * @return CompletableFuture which when completed will contain all positions to entities in the set. 
     */
    CompletableFuture<Map<Long, String>> getEntitiesWithPosition(String scope, String stream) {
        Map<Long, String> result = new ConcurrentHashMap<>();
        String collectionsPath = getCollectionsPath(scope, stream);
        return storeHelper.sync(collectionsPath)
                          .thenCompose(z -> {
                    return Futures.exceptionallyExpecting(storeHelper.getChildren(collectionsPath), DATA_NOT_FOUND_PREDICATE, Collections.emptyList())
                                  .thenCompose(children -> {
                                      // start with smallest collection and collect records
                                      List<Integer> iterable = children.stream().map(Integer::parseInt).collect(Collectors.toList());

                                      return Futures.loop(iterable, collectionNumber -> {
                                          return Futures.exceptionallyExpecting(storeHelper.getChildren(getEntitiesPath(scope, stream, collectionNumber)),
                                                  DATA_NOT_FOUND_PREDICATE, Collections.emptyList())
                                                        .thenCompose(entities -> Futures.allOf(
                                                                entities.stream().map(x -> {
                                                                    int pos = getPositionFromPath(x);
                                                                    return storeHelper.getData(getEntityPath(scope, stream, collectionNumber, pos),
                                                                            m -> new String(m, Charsets.UTF_8))
                                                                                      .thenAccept(r -> {
                                                                                          result.put(Position.toLong(collectionNumber, pos),
                                                                                                  r.getObject());
                                                                                      });
                                                                }).collect(Collectors.toList()))
                                                        ).thenApply(v -> true);
                                      }, executor);
                                  }).thenApply(v -> result)
                                  .whenComplete((r, e) -> {
                                      if (e != null) {
                                          log.error("error encountered while trying to retrieve entities for stream {}/{}", scope, stream, e);
                                      } else {
                                          log.debug("entities at positions {} retrieved for stream {}/{}", r, scope, stream);
                                      }
                                  });
                });

    }

    private String getStreamPath(String scope, String stream) {
        String scopePath = ZKPaths.makePath(ordererName, scope);
        return ZKPaths.makePath(scopePath, stream);
    }

    private String getCollectionsPath(String scope, String stream) {
        return ZKPaths.makePath(getStreamPath(scope, stream), COLLECTIONS_NODE);
    }

    private String getCollectionPath(String scope, String stream, int collectionNum) {
        return ZKPaths.makePath(getCollectionsPath(scope, stream), Integer.toString(collectionNum));
    }

    private String getCollectionSealedPath(String scope, String stream, Integer collectionNum) {
        return ZKPaths.makePath(getCollectionPath(scope, stream, collectionNum), SEALED_NODE);
    }

    private String getEntitiesPath(String scope, String stream, Integer collectionNum) {
        return ZKPaths.makePath(getCollectionPath(scope, stream, collectionNum), ENTITIES_NODE);
    }

    private String getEntitySequentialPath(String scope, String stream, Integer collectionNum) {
        return ZKPaths.makePath(getEntitiesPath(scope, stream, collectionNum), POSITION_NODE);
    }

    private String getEntityPath(String scope, String stream, int collectionNumber, int position) {
        return String.format("%s%010d", getEntitySequentialPath(scope, stream, collectionNumber), position);
    }

    private String getEntityPath(String scope, String stream, long entity) {
        Position position = new Position(entity);
        return getEntityPath(scope, stream, position.collectionNumber, position.positionInCollection);
    }

    private int getPositionFromPath(String name) {
        return Integer.parseInt(name.substring(name.length() - 10));
    }

    private CompletableFuture<Integer> getLatestCollection(String scope, String stream) {
        return storeHelper.getChildren(getCollectionsPath(scope, stream))
                          .thenCompose(children -> {
                              int latestcollectionNum = children.stream().mapToInt(Integer::parseInt).max().orElse(0);
                              return storeHelper.checkExists(getCollectionSealedPath(scope, stream, latestcollectionNum))
                                                .thenCompose(sealed -> {
                                                    if (sealed) {
                                                        return storeHelper.createZNodeIfNotExist(
                                                                getCollectionPath(scope, stream, latestcollectionNum + 1))
                                                                          .thenApply(v -> latestcollectionNum + 1);
                                                    } else {
                                                        return CompletableFuture.completedFuture(latestcollectionNum);
                                                    }
                                                });
                          });
    }

    /**
     * Collection should be sealed while calling this method
     * @param scope scope 
     * @param stream stream 
     * @param collectionNum collection to delete
     * @return future which when completed will have the collection deleted if it is empty or ignored otherwise. 
     */
    private CompletableFuture<Void> tryDeleteSealedCollection(String scope, String stream, Integer collectionNum) {
        // purge garbage znodes
        // attempt to delete entities node
        // delete collection
        return getLatestCollection(scope, stream)
                .thenCompose(latestcollectionNum ->
                        // 1. purge
                        storeHelper.getChildren(getEntitiesPath(scope, stream, collectionNum))
                                   .thenCompose(entitiesPos -> {
                                       String entitiesPath = getEntitiesPath(scope, stream, collectionNum);

                                       // delete entities greater than max pos
                                       return Futures.allOf(entitiesPos.stream().filter(pos -> getPositionFromPath(pos) > rollOverAfter)
                                                                    .map(pos -> storeHelper.deletePath(ZKPaths.makePath(entitiesPath, pos), false))
                                                                    .collect(Collectors.toList()));
                                   }))
                .thenCompose(x -> {
                    // 2. Try deleting entities root path. 
                    // if we are able to delete entities node then we can delete the whole thing
                    return Futures.exceptionallyExpecting(storeHelper.deletePath(getEntitiesPath(scope, stream, collectionNum), false)
                                                                     .thenCompose(v -> storeHelper.deleteTree(getCollectionPath(scope, stream, collectionNum))),
                            e -> Exceptions.unwrap(e) instanceof StoreException.DataNotEmptyException, null);
                });
    }
    
    @VisibleForTesting
    CompletableFuture<Boolean> isSealed(String scope, String stream, int collectionNum) {
        return Futures.exceptionallyExpecting(storeHelper.getData(getCollectionSealedPath(scope, stream, collectionNum), x -> x).thenApply(v -> true), 
            e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, false);        
    }
    
    @VisibleForTesting
    CompletableFuture<Boolean> isDeleted(String scope, String stream, int collectionNum) {
        return Futures.exceptionallyExpecting(storeHelper.getData(getCollectionPath(scope, stream, collectionNum), x -> x).thenApply(v -> false), 
            e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, true);
    }

    @VisibleForTesting
    CompletableFuture<Boolean> positionExists(String scope, String stream, long position) {
        return Futures.exceptionallyExpecting(storeHelper.getData(getEntityPath(scope, stream, position), x -> x).thenApply(v -> true), 
            e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, false);
    }
    
    @Data
    @AllArgsConstructor
    @VisibleForTesting
    static class Position {
        private final int collectionNumber;
        private final int positionInCollection;

        public Position(long position) {
            this.collectionNumber = (int) (position >> 32);
            this.positionInCollection = (int) position;
        }
        
        static long toLong(int collectionNumber, int positionInCollection) {
            return (long) collectionNumber << 32 | (positionInCollection & 0xFFFFFFFFL);
        }
    }
}
