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

import com.google.common.collect.Sets;

import java.util.Arrays;
import java.util.Set;

/**
 * This is used to represent the state of the Stream.
 */
public enum State {
    UNKNOWN,
    CREATING,
    ACTIVE,
    UPDATING,
    SCALING,
    COMMITTING_TXN,
    TRUNCATING,
    SEALING,
    SEALED;

    private enum StateTransitions {
        UNKNOWN(State.UNKNOWN, State.CREATING),
        CREATING(State.CREATING, State.ACTIVE),
        ACTIVE(State.ACTIVE, State.SCALING, State.TRUNCATING, State.COMMITTING_TXN, State.SEALING, State.SEALED, State.UPDATING),
        SCALING(State.SCALING, State.ACTIVE),
        COMMITTING_TXN(State.COMMITTING_TXN, State.ACTIVE),
        TRUNCATING(State.TRUNCATING, State.ACTIVE),
        UPDATING(State.UPDATING, State.ACTIVE),
        SEALING(State.SEALING, State.SEALED),
        SEALED(State.SEALED);

        private final Set<State> transitions;

        StateTransitions(State... states) {
            this.transitions = Sets.immutableEnumSet(Arrays.asList(states));
        }
    }

    public static boolean isTransitionAllowed(State currentState, State newState) {
        return StateTransitions.valueOf(currentState.name()).transitions.contains(newState);
    }
}
