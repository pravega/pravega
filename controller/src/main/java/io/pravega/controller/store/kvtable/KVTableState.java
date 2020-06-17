/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.kvtable;

import com.google.common.collect.Sets;

import java.util.Arrays;
import java.util.Set;

/**
 * This is used to represent the state of the KeyValueTable.
 */
public enum KVTableState {
    UNKNOWN,
    CREATING,
<<<<<<< HEAD
    DELETING,
=======
>>>>>>> Issue 4796: (KeyValue Tables) CreateAPI for Key Value Tables (#4797)
    ACTIVE;

    private enum StateTransitions {
        UNKNOWN(KVTableState.UNKNOWN, KVTableState.CREATING),
<<<<<<< HEAD
        CREATING(KVTableState.CREATING, KVTableState.ACTIVE, KVTableState.DELETING),
        ACTIVE(KVTableState.ACTIVE, KVTableState.DELETING),
        DELETING(KVTableState.DELETING);

=======
        CREATING(KVTableState.CREATING, KVTableState.ACTIVE),
        ACTIVE(KVTableState.ACTIVE);
>>>>>>> Issue 4796: (KeyValue Tables) CreateAPI for Key Value Tables (#4797)

        private final Set<KVTableState> transitions;

        StateTransitions(KVTableState... states) {
            this.transitions = Sets.immutableEnumSet(Arrays.asList(states));
        }
    }

    public static boolean isTransitionAllowed(KVTableState currentState, KVTableState newState) {
        return StateTransitions.valueOf(currentState.name()).transitions.contains(newState);
    }
}
