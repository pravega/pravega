/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.framework;

import com.google.gson.Gson;
import lombok.Builder;

import java.util.ArrayList;
import java.util.List;

import static com.emc.pravega.framework.LoginClient.MESOS_URL;

@Builder
public class LogFileDownloader {

    private String taskId;
    private String slaveId;

    private final AuthEnabledHttpClient client = new AuthEnabledHttpClient();

    public String getDirectoryPath() {
        String url = "https://10.240.124.2/agent/" + slaveId + "/slave(1)/state";
        //fetch the details of the slave
        String mesosInfo = client.getURL(url);
        SlaveState slaveState = new Gson().fromJson(mesosInfo, SlaveState.class);

        List<String> directoryPaths = new ArrayList<>(2);
        slaveState.getFrameworks().stream()
                .forEach(framework -> {
                    //search for task id in the executor.
                    framework.getExecutors().stream()
                            .filter(executor -> executor.getId().equals(taskId))
                            .forEach(executor -> directoryPaths.add(executor.getDirectory()));
                    if (directoryPaths.size() == 0) { // if not found then check completedExecutors
                        //the service might have crashed and marathon would have spawned a new instance
                        framework.getCompletedExecutors().stream()
                                .filter(executor -> executor.getId().equals(taskId))
                                .forEach(executor -> directoryPaths.add(executor.getDirectory()));
                    }
                });
        assert directoryPaths.size() <= 1 : "Directory paths must not be greater than 1";
        return directoryPaths.get(0);
    }

    public List<String> getFilesToBeDownloaded(final String directoryPath) {
        //check not null not empty.
        String url = MESOS_URL + "/agent/" + slaveId + "/files/browse?path=" + directoryPath;
        String result = client.getURL(url);
        // fetch the list of files.
        //check if they match the pattern stderr, stdout, .log
        //return the paths as a list
        return null;
    }
}
