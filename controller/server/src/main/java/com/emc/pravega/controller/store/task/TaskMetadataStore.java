/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.task;

import com.emc.pravega.controller.task.TaskData;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Task metadata store.
 */
public interface TaskMetadataStore {

    /**
     * Locks a resource for reads or updates.
     * If (oldOwner, oldTag) are specified then it revokes old owner's lock and itself acquires it.
     * This is non-reentrant lock, i.e., a process/thread cannot lock the same resource twice.
     * If oldOwner is null then it first creates a node LOCKTYPE<SEQ_NUMBER> having data (owner, tag, taskData)
     * as a child of /taskRoot/resource.
     *
     * Then it waits until the following happens
     * 1. For WRITE lock type, it waits until SEQ_NUMBER is the smallest sequence number
     *    among all children of /taskRoot/resource
     * 2. For READ lock type it waits until no child of /taskRoot/resource with write lock
     *    type has a smaller sequence number than SEQ_NUMBER
     *
     * If oldOwner is non-null then it first replaces data of node LOCKTYPE<seqNumber> from (oldOwner, oldTag, taskData)
     * to (owner, tag, taskData).
     *
     * Then it waits until the conditions mentioned above are satisfied.
     *
     * @param resource resource identifier.
     * @param type     lock type.
     * @param taskData details of update task on the resource.
     * @param owner    owner of the task.
     * @param tag      tag.
     * @param seqNumber optional sequence number in case the lock was previously held by some other host.
     * @param oldOwner host that had previously locked the resource.
     * @param oldTag   tag that took the lock
     * @return sequence number of the lock node when lock is acquired, throws LockFailedException on error.
     */
    CompletableFuture<Integer> lock(final Resource resource,
                                    final LockType type,
                                    final TaskData taskData,
                                    final String owner,
                                    final String tag,
                                    final Optional<Integer> seqNumber,
                                    final String oldOwner,
                                    final String oldTag);

    /**
     * Unlocks a resource if it is owned by the specified owner (owner, tag) by deleting the child
     * LOCKTYPE<seqNumber> of /taskRoot/resource
     *
     * Delete the key value pair LOCKTYPE<seqNumber> -> (owner', tag', taskData) iff owner' == owner and tag' == tag.
     *
     * @param resource resource identifier.
     * @param type     lock type.
     * @param seqNumber sequence number returned by the lock method.
     * @param owner    owner of the lock.
     * @param tag      tag.
     * @return void if successful, otherwise throws UnlockFailedException.
     */
    CompletableFuture<Void> unlock(final Resource resource,
                                   final LockType type,
                                   final int seqNumber,
                                   final String owner,
                                   final String tag);

    /**
     * Fetch details of task associated with the specified resource and locked/owned by specified owner and tag, along
     * with the sequence number of the lock node.
     *
     * @param resource resource.
     * @param owner    owner.
     * @param tag      tag.
     * @return TaskData and lock node's sequence number, if owner and tag have made a lock attempt on the specified
     *         resource otherwise Optional.empty().
     */
    CompletableFuture<Optional<Pair<TaskData, Integer>>> getTask(final Resource resource, final String owner, final String tag);

    /**
     * Adds specified resource as a child of current host's hostId node.
     * This is idempotent operation.
     *
     * @param parent Parent node.
     * @param child  TaggedResource node to be added as child of parent.
     * @return void in future.
     */
    CompletableFuture<Void> putChild(final String parent, final TaggedResource child);

    /**
     * Removes the specified child node from the specified parent node.
     * This is idempotent operation.
     * If deleteEmptyParent is true and parent has no child after deletion of given child then parent is also deleted.
     *
     * @param parent            node whose child is to be removed.
     * @param child             child TaggedResource node to remove.
     * @param deleteEmptyParent to delete or not to delete.
     * @return void in future.
     */
    CompletableFuture<Void> removeChild(final String parent, final TaggedResource child, final boolean deleteEmptyParent);

    /**
     * Remove a parent node if it is empty.
     * This is idempotent operation.
     *
     * @param parent parent node.
     * @return void in future.
     */
    CompletableFuture<Void> removeNode(final String parent);

    /**
     * Returns a random child from among the children of specified parent.
     *
     * @param parent parent node.
     * @return A randomly selected child if parent has children, otherwise Optional.empty().
     */
    CompletableFuture<Optional<TaggedResource>> getRandomChild(final String parent);
}
