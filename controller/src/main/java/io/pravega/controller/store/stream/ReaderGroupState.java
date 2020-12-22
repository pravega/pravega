/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
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
 * This is used to represent the state of the ReaderGroup.
 */
public enum ReaderGroupState {
    UNKNOWN,
    CREATING,
    DELETING,
    ACTIVE;

    private enum StateTransitions {
        UNKNOWN(ReaderGroupState.UNKNOWN, ReaderGroupState.CREATING),
        CREATING(ReaderGroupState.CREATING, ReaderGroupState.ACTIVE, ReaderGroupState.DELETING),
        ACTIVE(ReaderGroupState.ACTIVE, ReaderGroupState.DELETING),
        DELETING(ReaderGroupState.DELETING);

        private final Set<ReaderGroupState> transitions;

        StateTransitions(ReaderGroupState... states) {
            this.transitions = Sets.immutableEnumSet(Arrays.asList(states));
        }
    }

    public static boolean isTransitionAllowed(ReaderGroupState currentState, ReaderGroupState newState) {
        return StateTransitions.valueOf(currentState.name()).transitions.contains(newState);
    }
}
