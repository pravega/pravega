/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.framework;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Response;
import okio.BufferedSink;
import okio.Okio;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.emc.pravega.framework.AuthEnabledHttpClient.getURL;
import static com.emc.pravega.framework.LoginClient.MESOS_URL;

@Slf4j
public class LogFileDownloader {

    private static final String MESOS_DIRECTORY_URL = "%s/agent/%s/files/browse?path=%s";

    public static String getDirectoryPath(final String slaveId, final String taskId) {

        String url = MESOS_URL + "/agent/" + slaveId + "/slave(1)/state";
        //fetch the details of the slave
        String mesosInfo = null;
        try {
            mesosInfo = getURL(url).get().body().string();
        } catch (IOException | InterruptedException | ExecutionException e) {
            throw new TestFrameworkException(TestFrameworkException.Type.RequestFailed, "Error while getting slave " +
                    "information", e);
        }
        SlaveState slaveState = new Gson().fromJson(mesosInfo, SlaveState.class);

        List<String> directoryPaths = new ArrayList<>();
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

    public static CompletableFuture<List<String>> getFilesToBeDownloaded(final String slaveId, final String
            directoryPath) {

        Exceptions.checkNotNullOrEmpty(slaveId, "slaveId");
        Exceptions.checkNotNullOrEmpty(directoryPath, "directoryPath");

        String url = String.format(MESOS_DIRECTORY_URL, MESOS_URL, slaveId, directoryPath);

        return getURL(url).thenApply(response -> {
            try {
                List<String> filePaths = new ArrayList<>();

                String fileListJson = response.body().string();
                JsonArray filePathJsonArray = new JsonParser().parse(fileListJson).getAsJsonArray();
                filePathJsonArray.forEach(jsonElement -> filePaths.add(jsonElement.getAsJsonObject().get("path")
                        .getAsString()));
                return filePaths;

            } catch (IOException e) {
                throw new TestFrameworkException(TestFrameworkException.Type.InternalError, "Error while listing " +
                        "files in a given directory", e);
            }
        });
    }

    public static CompletableFuture<Void> downloadFile(final String destinationDirectory, final String slaveId,
                                                       final String taskId, final String filePath) {

        Path url = Paths.get(MESOS_URL + "/agent/" + slaveId + "/files/download?path=" + filePath);

        return getURL(url.toString()).thenApply(response -> {

            try {
                //create the required file .
                Path fp = Paths.get(destinationDirectory, taskId, url.getFileName().toString());
                File file = fp.toFile();
                if (!fp.toFile().exists()) {
                    Files.createFile(fp);
                }
                // ensure sink.close() is invoked to release resources
                try (BufferedSink sink = Okio.buffer(Okio.sink(fp.toFile()))) {
                    sink.writeAll(response.body().source());
                }
                return null;

            } catch (IOException e) {
                throw new TestFrameworkException(TestFrameworkException.Type.InternalError, "Error while downloading " +
                        "files in a given path", e);
            }
        });
    }

    public static CompletableFuture<Void> downloadServiceLogs(final String appId, final String directoryName) {
        log.info("Download service logs for service :{}", appId);
        CompletableFuture<JsonArray> taskInfo = getTaskInfo(appId);

        CompletableFuture<Void> result = taskInfo.thenCompose(tasks -> {
            List<CompletableFuture<Void>> instanceDownloadResults = new ArrayList<>();
            tasks.forEach(task -> {
                JsonObject taskData = task.getAsJsonObject();
                //read taskId and slaveId from the json object.
                final String taskId = taskData.get("id").getAsString();
                final String slaveId = taskData.get("slaveId").getAsString();
                log.info("Service : {} has the following task :{} running on slave:{}", appId, taskId, slaveId);

                final String directoryPath = getDirectoryPath(slaveId, taskId);
                log.info("Directory Path: {}", directoryPath);

                try {
                    Path fp = Paths.get(directoryName, taskId);
                    Files.createDirectories(fp);
                } catch (IOException e) {
                    throw new TestFrameworkException(TestFrameworkException.Type.InternalError, "Failed to create" +
                            " directories while downloading logs", e);
                }

                CompletableFuture<Void> instanceDownloadResult = getFilesToBeDownloaded(slaveId, directoryPath)
                        .thenCompose(fileList -> {
                            List<CompletableFuture<Void>> r6 = fileList.stream().map(s -> downloadFile(directoryName,
                                    slaveId, taskId, s))
                                    .collect(Collectors.toList());
                            return FutureHelpers.allOf(r6);
                        });
                instanceDownloadResults.add(instanceDownloadResult);
            });
            return FutureHelpers.allOf(instanceDownloadResults);
        });
        return result;
    }

    private static CompletableFuture<JsonArray> getTaskInfo(String appId) {

        final CompletableFuture<Response> taskInfoResponse = getURL(MESOS_URL + "/service/marathon/v2/apps/" + appId +
                "?embed=apps.tasks");

        CompletableFuture<JsonArray> taskInfo = taskInfoResponse.thenApply(response -> {
            try {
                JsonObject r = new JsonParser().parse(response.body().string()).getAsJsonObject();
                //read the tasks info for the given app
                final Optional<JsonArray> jsonElements = Optional.of(r.getAsJsonObject("app")).flatMap(jsonObject ->
                        Optional.of(jsonObject.getAsJsonArray("tasks")));
                return jsonElements.orElse(new JsonArray());
            } catch (IOException e) {
                throw new TestFrameworkException(TestFrameworkException.Type.InternalError, "Error while fetching " +
                        "task info for service", e);
            }
        });

        return taskInfo;
    }
}
