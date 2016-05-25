package com.emc.logservice.server;

import java.util.StringJoiner;

/**
 * Defines various States that a Container can be in.
 */
public enum ContainerState {
    /**
     * Container has just been created, but not yet initialized.
     */
    Created,

    /**
     * Container has been initialized and is ready to start processing.
     */
    Initialized,

    /**
     * Container has been started and is currently running.
     */
    Started,

    /**
     * Container is not running, but was running at a previous point in time.
     */
    Stopped;

    private ContainerState[] validPreviousStates;

    static {
        Created.validPreviousStates = new ContainerState[0];
        Initialized.validPreviousStates = new ContainerState[]{ ContainerState.Created };
        Started.validPreviousStates = new ContainerState[]{ ContainerState.Initialized, ContainerState.Stopped };
        Stopped.validPreviousStates = new ContainerState[]{ ContainerState.Started };
    }

    public void checkValidPreviousState(ContainerState previousState) {
        for (ContainerState state : this.validPreviousStates) {
            if (previousState == state) {
                // Validation passed.
                return;
            }
        }

        // TODO: is there a generic version of StringJoiner that calls 'toString' on target objects?
        StringJoiner joiner = new StringJoiner(", ");
        for (ContainerState s : this.validPreviousStates) {
            joiner.add(s.toString());
        }

        throw new IllegalStateException(String.format("Container is in an invalid state in order to transition to '%s'. Expected: '%s', Actual: '%s'.", this, joiner.toString(), previousState));
    }
}
