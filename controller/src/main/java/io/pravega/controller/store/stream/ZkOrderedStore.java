/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.utils.ZKPaths;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static io.pravega.controller.store.stream.ZKStreamMetadataStore.DATA_NOT_FOUND_PREDICATE;

/**
 * This class is used to store ordered objects on a per stream basis. 
 * It uses Persistent Sequential znodes to create ordered sets.
 * New entities can be added and existing entities can be removed from these ordered sets. 
 * All entities from the set can be viewed and ordered based on the given order (a long). 
 * 
 * Since we use Persistent Sequential znodes to assign order, we can exhaust the order space which is basically 10 digits
 * (Integer.Max). So this store is responsible for internally creating and progressing new sets once a previous set is 
 * exhausted. Overall we can create Integer.Max new sets. This means overall we have approximately Long.Max entries that 
 * each ordered set can support in its lifetime. 
 */
@Slf4j
public class ZkOrderedStore {
    public static final String SEALED_NODE = "sealed";
    public static final String ENTITIES_NODE = "entities";
    public static final String POSITION_NODE = "pos";
    private final ZKStoreHelper storeHelper;
    private final String ordererName;
    private final Executor executor;
    private final int rollOverAfter;

    // root/scope/stream/<queueNum>/sealed
    // root/scope/stream/<queueNum>/entities
    public ZkOrderedStore(String ordererName, ZKStoreHelper storeHelper, Executor executor) {
        // default roll over is after 90% or 1.8 billion entries have been added to a set. 
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
     * Method to add new entity to the ordered set. Note: Same entity could be added to the set multiple times.
     * Entities are added to the latest set for the stream. If the set has exhausted positions allowed for by rollOver limit,
     * a new successor set is initiated and the entry is added to the new set.
     * Any entries with positions higher than rollover values under a set are ignored and eventually purged. 
     * 
     * @param scope scope
     * @param stream stream 
     * @param entity entity to add
     * @return CompletableFuture which when completed returns the position where the entity is added to the set. 
     */
    public CompletableFuture<Long> addEntity(String scope, String stream, String entity) {
        // add persistent sequential node to the latest queue number 
        // if queueNum is sealed, increment queue number and write the entity there. 
        return getLatestQueue(scope, stream)
                .thenCompose(latestQueueNum ->
                        storeHelper.createPersistentSequentialZNode(getEntitySequentialPath(scope, stream, latestQueueNum),
                                entity.getBytes(Charsets.UTF_8))
                                   .thenCompose(positionPath -> {
                                       int position = getPositionFromPath(positionPath);
                                       if (position > rollOverAfter) {
                                           // if newly created position exceeds rollover limit, we need to delete that entry 
                                           // and roll over. 
                                           // 1. delete newly created path
                                           return storeHelper.deletePath(positionPath, false)
                                                             // 2. seal latest queue
                                                             .thenCompose(v -> storeHelper.createZNodeIfNotExist(
                                                                     getQueueSealedPath(scope, stream, latestQueueNum)))
                                                             // 3. call addEntity recursively
                                                             .thenCompose(v -> addEntity(scope, stream, entity))
                                                             // 4. delete empty sealed queue path
                                                             .thenCompose(orderedPosition -> 
                                                                     tryDeleteSealedQueuePath(scope, stream, latestQueueNum)
                                                                     .thenApply(v -> orderedPosition));

                                       } else {
                                           return CompletableFuture.completedFuture(Position.toLong(latestQueueNum, position));
                                       }
                                   }))
                .whenComplete((r, e) -> {
                    if (e != null) {
                        log.error("error encountered while trying to add entity {} for stream {}/{}", entity, scope, stream, e);
                    } else {
                        log.debug("entity {} added for stream {}/{} at position {}", entity, scope, stream, r);
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
    public CompletableFuture<Void> removeEntities(String scope, String stream, Collection<Long> entities) {
        Set<Integer> queues = entities.stream().collect(Collectors.groupingBy(x -> new Position(x).queueNumber)).keySet();
        return Futures.allOf(entities.stream()
                                     .map(entity -> storeHelper.deletePath(getEntityPath(scope, stream, entity), false))
                                     .collect(Collectors.toList()))
                      .thenCompose(v -> Futures.allOf(queues.stream().map(queueNum -> tryDeleteSealedQueuePath(scope, stream, queueNum))
                                                            .collect(Collectors.toList())))
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
    public CompletableFuture<Map<Long, String>> getEntitiesWithPosition(String scope, String stream) {
        Map<Long, String> result = new ConcurrentHashMap<>();
        return Futures.exceptionallyExpecting(storeHelper.getChildren(getStreamPath(scope, stream)), DATA_NOT_FOUND_PREDICATE, Collections.emptyList())
                          .thenCompose(children -> {
                              // start with smallest queue and collect records
                              Iterator<Integer> iterator = children.stream().mapToInt(Integer::parseInt).iterator();

                              return Futures.loop(iterator::hasNext, () -> {
                                  Integer queueNumber = iterator.next();
                                  return Futures.exceptionallyExpecting(storeHelper.getChildren(getEntitiesPath(scope, stream, queueNumber)),
                                                    DATA_NOT_FOUND_PREDICATE, Collections.emptyList())
                                                    .thenCompose(entities -> Futures.allOf(
                                                                    entities.stream().map(x -> {
                                                                        int pos = getPositionFromPath(x);
                                                                        return storeHelper.getData(getEntityPath(scope, stream, queueNumber, pos))
                                                                                          .thenAccept(r -> {
                                                                                              result.put(Position.toLong(queueNumber, pos),
                                                                                                      new String(r.getData(), Charsets.UTF_8));
                                                                                          });
                                                                    }).collect(Collectors.toList()))
                                                    );
                              }, executor);
                          }).thenApply(v -> result)
                      .whenComplete((r, e) -> {
                          if (e != null) {
                              log.error("error encountered while trying to retrieve entities for stream {}/{}", scope, stream, e);
                          } else {
                              log.debug("entities at positions {} retrieved for stream {}/{}", r, scope, stream);
                          }
                      });

    }

    private String getStreamPath(String scope, String stream) {
        String scopePath = ZKPaths.makePath(ordererName, scope);
        return ZKPaths.makePath(scopePath, stream);
    }

    private String getQueuePath(String scope, String stream, Integer queueNum) {
        return ZKPaths.makePath(getStreamPath(scope, stream), queueNum.toString());
    }

    private String getQueueSealedPath(String scope, String stream, Integer queueNum) {
        return ZKPaths.makePath(getQueuePath(scope, stream, queueNum), SEALED_NODE);
    }

    private String getEntitiesPath(String scope, String stream, Integer queueNum) {
        return ZKPaths.makePath(getQueuePath(scope, stream, queueNum), ENTITIES_NODE);
    }

    private String getEntitySequentialPath(String scope, String stream, Integer queueNum) {
        return ZKPaths.makePath(getEntitiesPath(scope, stream, queueNum), POSITION_NODE);
    }

    private String getEntityPath(String scope, String stream, int queueNumber, int position) {
        return String.format("%s%010d", getEntitySequentialPath(scope, stream, queueNumber), position);
    }

    private String getEntityPath(String scope, String stream, long entity) {
        Position position = new Position(entity);
        return getEntityPath(scope, stream, position.queueNumber, position.positionInQueue);
    }

    private int getPositionFromPath(String name) {
        return Integer.parseInt(name.substring(name.length() - 10));
    }

    private CompletableFuture<Integer> getLatestQueue(String scope, String stream) {
        return storeHelper.getChildren(getStreamPath(scope, stream))
                          .thenCompose(children -> {
                              int latestQueueNum = children.stream().mapToInt(Integer::parseInt).max().orElse(0);
                              return storeHelper.checkExists(getQueueSealedPath(scope, stream, latestQueueNum))
                                                .thenCompose(sealed -> {
                                                    if (sealed) {
                                                        return storeHelper.createZNodeIfNotExist(
                                                                getQueuePath(scope, stream, latestQueueNum + 1))
                                                                          .thenApply(v -> latestQueueNum + 1);
                                                    } else {
                                                        return CompletableFuture.completedFuture(latestQueueNum);
                                                    }
                                                });
                          });
    }

    private CompletableFuture<Void> tryDeleteSealedQueuePath(String scope, String stream, Integer queueNum) {
        // if higher queue number exists then we can attempt to delete this node
        // purge garbage znodes
        // attempt to delete entities node
        // delete queue
        return getLatestQueue(scope, stream)
                .thenCompose(latestQueueNum ->
                        // 1. purge
                        storeHelper.getChildren(getEntitiesPath(scope, stream, queueNum))
                                   .thenCompose(entitiesPos -> {
                                       String entitiesPath = getEntitiesPath(scope, stream, queueNum);

                                       // delete entities greater than max pos
                                       return Futures.allOf(entitiesPos.stream().filter(pos -> getPositionFromPath(pos) > rollOverAfter)
                                                                    .map(pos -> storeHelper.deletePath(ZKPaths.makePath(entitiesPath, pos), false))
                                                                    .collect(Collectors.toList()));
                                   }))
                .thenCompose(x -> {
                    // 2. Try deleting entities root path. 
                    // if we are able to delete entities node then we can delete the whole thing
                    return Futures.exceptionallyExpecting(storeHelper.deletePath(getEntitiesPath(scope, stream, queueNum), false)
                                                                     .thenCompose(v -> storeHelper.deleteTree(getQueuePath(scope, stream, queueNum))),
                            e -> Exceptions.unwrap(e) instanceof StoreException.DataNotEmptyException, null);
                });
    }
    
    @VisibleForTesting
    CompletableFuture<Boolean> isSealed(String scope, String stream, int queueNumber) {
        return Futures.exceptionallyExpecting(storeHelper.getData(getQueueSealedPath(scope, stream, queueNumber)).thenApply(v -> true), 
            e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, false);        
    }
    
    @VisibleForTesting
    CompletableFuture<Boolean> isDeleted(String scope, String stream, int queueNumber) {
        return Futures.exceptionallyExpecting(storeHelper.getData(getQueuePath(scope, stream, queueNumber)).thenApply(v -> false), 
            e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, true);
    }

    @VisibleForTesting
    CompletableFuture<Boolean> positionExists(String scope, String stream, long position) {
        return Futures.exceptionallyExpecting(storeHelper.getData(getEntityPath(scope, stream, position)).thenApply(v -> true), 
            e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, false);
    }
    
    @Data
    @AllArgsConstructor
    @VisibleForTesting
    static class Position {
        private final int queueNumber;
        private final int positionInQueue;

        public Position(long position) {
            this.queueNumber = (int) (position >> 32);
            this.positionInQueue = (int) position;
        }
        
        static long toLong(int queueNumber, int positionInQueue) {
            return (long) queueNumber << 32 | (positionInQueue & 0xFFFFFFFFL);
        }
    }
}
