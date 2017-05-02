/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package io.pravega.test.system.framework.tasks;

/**
 * Task abstraction for the System test framework.
 * System test framework can execute task one time activity on the cluster.
 * E.g: CommandTask is an implementation of the Task which is used to execute custom commands on the cluster.
 *
 */
public interface Task {
    /**
     * Start a given task.
     *
     */
    public void start();

    /**
     * Stop a task.
     */
    public void stop();

    /**
     * Return the ID of the task.
     *
     * @return ID of the task.
     */
    public String getID();
}
