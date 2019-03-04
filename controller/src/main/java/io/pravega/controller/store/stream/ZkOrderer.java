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
import io.pravega.controller.store.stream.records.OrderedEntity;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.utils.ZKPaths;

import java.util.ArrayList;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ZkOrderer {
    public static final String SEALED_NODE = "sealed";
    public static final String ENTITIES_NODE = "entities";
    public static final String POSITION_NODE = "pos";
    private final ZKStoreHelper storeHelper;
    private final String ordererName;
    private final Executor executor;
    private final int rollOverAfter;

    // root/scope/stream/<queueNum>/sealed
    // root/scope/stream/<queueNum>/entities
    public ZkOrderer(String ordererName, ZKStoreHelper storeHelper, Executor executor) {
        this(ordererName, storeHelper, executor, (Integer.MAX_VALUE / 10) * 9);
    }

    @VisibleForTesting
    ZkOrderer(String ordererName, ZKStoreHelper storeHelper, Executor executor, int rollOverAfter) {
        this.ordererName = ordererName;
        this.storeHelper = storeHelper;
        this.executor = executor;
        this.rollOverAfter = rollOverAfter;
    }

    public CompletableFuture<Void> addToOrderer(String scope, String stream, String entity) {
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
                                                             // 3. call addToOrderer recursively
                                                             .thenCompose(v -> addToOrderer(scope, stream, entity))
                                                             // 4. delete empty sealed queue path
                                                             .thenCompose(v -> tryDeleteSealedQueuePath(scope, stream, latestQueueNum));

                                       } else {
                                           return CompletableFuture.completedFuture(null);
                                       }
                                   }));
    }

    public CompletableFuture<Void> removeFromOrderer(String scope, String stream, List<OrderedEntity> entities) {
        Set<Integer> queues = entities.stream().collect(Collectors.groupingBy(OrderedEntity::getQueueNumber)).keySet();
        return Futures.allOf(entities.stream()
                                     .map(entity -> storeHelper.deletePath(getEntityPath(scope, stream, entity), false))
                                     .collect(Collectors.toList()))
                      .thenCompose(v -> Futures.allOf(queues.stream().map(queueNum -> tryDeleteSealedQueuePath(scope, stream, queueNum))
                                                            .collect(Collectors.toList())));
    }

    public CompletableFuture<List<Pair<String, OrderedEntity>>> getOrderedList(String scope, String stream) {
        List<Pair<String, OrderedEntity>> result = new ArrayList<>();
        return storeHelper.getChildren(getStreamPath(scope, stream))
                          .thenCompose(children -> {
                              IntStream sorted = children.stream().mapToInt(Integer::parseInt).sorted();

                              PrimitiveIterator.OfInt iterator = sorted.iterator();

                              return Futures.loop(iterator::hasNext, () -> {
                                  Integer queueNumber = iterator.next();
                                  return storeHelper.getChildren(getEntitiesPath(scope, stream, queueNumber))
                                                    .thenCompose(entities -> Futures.allOfWithResults(
                                                            entities.stream().map(x -> {
                                                                int pos = getPositionFromPath(x);
                                                                return storeHelper.getData(getEntityPath(scope, stream, queueNumber, pos))
                                                                                  .thenApply(r -> {
                                                                                      return new ImmutablePair<>(
                                                                                              new String(r.getData(), Charsets.UTF_8), 
                                                                                              new OrderedEntity(queueNumber, pos));
                                                                                  });
                                                            }).collect(Collectors.toList())))
                                                    .thenAccept(result::addAll);
                              }, executor);
                          }).thenApply(v -> result);
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
        return getEntitySequentialPath(scope, stream, queueNumber) + position;
    }

    private String getEntityPath(String scope, String stream, OrderedEntity entity) {
        return getEntitySequentialPath(scope, stream, entity.getQueueNumber()) + entity.getPositionInQueue();
    }

    private int getPositionFromPath(String name) {
        // TODO: shivesh verify
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
                                       return Futures.allOf(entitiesPos.stream().filter(pos -> getPositionFromPath(pos) >= rollOverAfter)
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
}
