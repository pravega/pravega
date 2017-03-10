/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.framework;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.framework.mesos.model.v1.Framework;
import com.emc.pravega.framework.mesos.model.v1.SlaveState;
import com.emc.pravega.framework.mesos.model.v1.Task;
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
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.emc.pravega.framework.AuthEnabledHttpClient.getURL;
import static com.emc.pravega.framework.LoginClient.MESOS_URL;

@Slf4j
public class LogFileDownloader {

    private static final String MESOS_DIRECTORY_URL = "%s/agent/%s/files/browse?path=%s";
    private static final String MESOS_MASTER_STATE = MESOS_URL + "/mesos/master/state";

    private static List<String> getDirectoryPaths(final String slaveId, final String taskId) {

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
        final Consumer<Framework> searchTaskInFramework = framework -> {
            //search for task id in the executor.
            framework.getExecutors().stream()
                    .filter(executor -> executor.getId().contains(taskId))
                    .forEach(executor -> directoryPaths.add(executor.getDirectory()));
            if (directoryPaths.size() == 0) { // if not found then check completedExecutors
                //the service might have crashed and marathon would have spawned a new instance
                framework.getCompletedExecutors().stream()
                        .peek(executor -> System.out.println("==>" + executor.getId()))
                        .filter(executor -> executor.getId().contains(taskId))
                        .peek(executor -> System.out.println(executor.getId()))
                        .forEach(executor -> directoryPaths.add(executor.getDirectory()));
            }
        };

        //search in active frameworks
        slaveState.getFrameworks().stream().forEach(searchTaskInFramework);
        if (directoryPaths.size() == 0) {
            //no paths found, search for completed frameworks . Usually metronome related tasks are found here.
            slaveState.getCompletedFrameworks().stream().forEach(searchTaskInFramework);
        }

        //throw an exception since no mesosDir is found.
        if (directoryPaths.size() == 0) {
            throw new TestFrameworkException(TestFrameworkException.Type.InternalError, "Failed to find the logs for " +
                    "the given task");
        }

        return directoryPaths;
    }

    /**
     * Download logs of a given service.
     *
     * @param serviceName    Name of the service managed using marathon.
     * @param destinationDir Name of the directory to which logs need to be downloaded.
     * @return it returns a CompletableFuture to indicate the status of the download.
     */
    public static CompletableFuture<Void> downloadServiceLogs(final String serviceName, final String destinationDir) {

        log.info("Download service logs for service :{}", serviceName);
        CompletableFuture<JsonArray> taskInfo = getTaskInfo(serviceName);

        CompletableFuture<Void> result = taskInfo.thenCompose(tasks -> {
            List<CompletableFuture<Void>> instanceDownloadResults = new ArrayList<>();
            tasks.forEach(task -> {
                JsonObject taskData = task.getAsJsonObject();
                //read taskId and slaveId from the json object.
                final String taskId = taskData.get("id").getAsString();
                final String slaveId = taskData.get("slaveId").getAsString();
                log.info("Service : {} has the following task :{} running on slave:{}", serviceName, taskId, slaveId);

                final String directoryPath = getDirectoryPaths(slaveId, taskId).get(0); //first path is the latest run
                log.info("Directory Path: {}", directoryPath);

                CompletableFuture<Void> instanceDownloadResult = downloadFiles(destinationDir, directoryPath, taskId,
                        slaveId);
                instanceDownloadResults.add(instanceDownloadResult);
            });
            return FutureHelpers.allOf(instanceDownloadResults);
        });
        return result;
    }

    /**
     * Download logs of a given test.
     *
     * @param testName       Name of the test.
     * @param destinationDir Name of the directory to which logs need to be downloaded.
     * @return it returns a CompletableFuture to indicate the status of the download.
     */
    public static CompletableFuture<Void> downloadTestLogs(final String testName, final String destinationDir) {
        log.info("Download test execution logs for test id: {} ", testName);

        return getTestTaskInfo(testName).thenCompose(task -> {
            String mesosDir = getDirectoryPaths(task.getSlaveId(), task.getId()).get(0);
            CompletableFuture<Void> downloadResult = downloadFiles(destinationDir, mesosDir, task.getId(), task
                    .getSlaveId());
            return downloadResult;
        });
    }

    private static CompletableFuture<Task> getTestTaskInfo(String testName) {
        return getURL(MESOS_MASTER_STATE).thenApply(response -> {
            try {
                SlaveState slaveState = new Gson().fromJson(response.body().string(), SlaveState.class);
                return slaveState.getFrameworks().stream()
                        //tests are executed by Metronome platform.
                        .filter(framework -> framework.getName().equalsIgnoreCase("metronome")).findFirst()
                        .map(framework -> framework.getCompletedTasks())
                        .map(tasks -> {
                            //fetch task information for the given test.
                            List<Task> testTasks = tasks.stream().filter(task -> task.getId().contains(testName))
                                    .collect(Collectors.toList());
                            assert testTasks.size() > 0 : "No task information for the given test";
                            return testTasks.get(testTasks.size() - 1);
                        }).orElseThrow(() -> new TestFrameworkException(TestFrameworkException.Type
                                .RequestFailed, "No task present for test"));

            } catch (IOException e) {
                throw new TestFrameworkException(TestFrameworkException.Type.InternalError, "Exception while " +
                        "converting the response to String");
            }
        });
    }

    private static CompletableFuture<List<String>> getFilesToBeDownloaded(final String slaveId, final String mesosDir) {

        Exceptions.checkNotNullOrEmpty(slaveId, "slaveId");
        Exceptions.checkNotNullOrEmpty(mesosDir, "mesosDir");

        String mesosDirEndpoint = String.format(MESOS_DIRECTORY_URL, MESOS_URL, slaveId, mesosDir);

        return getURL(mesosDirEndpoint).thenApply(response -> {
            try {
                List<String> filePaths = new ArrayList<>();

                String fileListJson = response.body().string();
                JsonArray filePathJsonArray = new JsonParser().parse(fileListJson).getAsJsonArray();
                filePathJsonArray.forEach(jsonElement -> filePaths.add(jsonElement.getAsJsonObject().get("path")
                        .getAsString()));
                filePaths.removeIf(path -> path.contains(".jar")); // do not add paths with .jar.
                return filePaths;

            } catch (IOException e) {
                throw new TestFrameworkException(TestFrameworkException.Type.InternalError, "Error while listing " +
                        "files in a given directory", e);
            }
        });
    }

    private static CompletableFuture<Void> downloadFile(final String destinationDir, final String slaveId,
                                                        final String taskId, final String filePath) {

        Path mesosFileEndpoint = Paths.get(MESOS_URL + "/agent/" + slaveId + "/files/download?path=" + filePath);

        return getURL(mesosFileEndpoint.toString()).thenApply(response -> {

            try {
                //create the required file .
                Path fp = Paths.get(destinationDir, taskId, mesosFileEndpoint.getFileName().toString());
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

    private static CompletableFuture<Void> downloadFiles(final String destinationDir, final String mesosDir, String
            taskId, String slaveId) {
        try {
            Path fp = Paths.get(destinationDir, taskId);
            Files.createDirectories(fp);
        } catch (IOException e) {
            throw new TestFrameworkException(TestFrameworkException.Type.InternalError, "Failed to create" +
                    " directories while downloading logs", e);
        }

        return getFilesToBeDownloaded(slaveId, mesosDir)
                .thenCompose(fileList -> {
                    List<CompletableFuture<Void>> fileDownloads = fileList.stream()
                            .map(s -> downloadFile(destinationDir, slaveId, taskId, s))
                            .collect(Collectors.toList());
                    return FutureHelpers.allOf(fileDownloads);
                });
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
