/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.framework;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.Builder;
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
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.emc.pravega.framework.LoginClient.MESOS_URL;

@Builder
public class LogFileDownloader {

    private String taskId;
    private String slaveId;

    private final AuthEnabledHttpClient client = new AuthEnabledHttpClient();

    public String getDirectoryPath() {

        String url = MESOS_URL + "/agent/" + slaveId + "/slave(1)/state";
        //fetch the details of the slave
        String mesosInfo = null;
        try {
            mesosInfo = client.getURL(url).get().body().string();
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

    public List<String> getFilesToBeDownloaded(final String directoryPath) throws IOException, ExecutionException,
            InterruptedException {
        // TODO: check not null not empty.

        String url = MESOS_URL + "/agent/" + slaveId + "/files/browse?path=" + directoryPath;
        String fileListJson = client.getURL(url).get().body().string();
        JsonArray r1 = new JsonParser().parse(fileListJson).getAsJsonArray();
        List<String> filePaths = new ArrayList<>(5);
        r1.forEach(jsonElement -> filePaths.add(jsonElement.getAsJsonObject().get("path").getAsString()));

        List<String> filteredFilePath = filePaths.stream()
                .filter(s -> s.contains("stderr") || s.contains("stdout")
                        || s.contains(".log")).collect(Collectors.toList());

        return filteredFilePath;
    }

    public void downloadFile(final String testName, final String slaveId, final String taskId, final String filePath)
            throws
            IOException, ExecutionException, InterruptedException {
        Path path = Paths.get(MESOS_URL + "/agent/" + slaveId + "/files/download?path=" + filePath);

        try (Response response = client.getURL(MESOS_URL + "/agent/" + slaveId + "/files/download?path=" + filePath)
                .get()) {
            Path fp = Paths.get(testName, taskId, path.getFileName().toString());
            File file = fp.toFile();
            Files.createDirectories(fp.getParent());
            if (!fp.toFile().exists()) {
                Files.createFile(fp);
            }

            BufferedSink sink = Okio.buffer(Okio.sink(fp.toFile()));
            // you can access body of response
            sink.writeAll(response.body().source());
            sink.close();
        }
    }

    public void downloadServiceLogs(String appId) {
        try {
            Optional<JsonArray> r1 = getTaskInfo(appId);

            r1.ifPresent(tasks -> tasks.forEach(task -> {
                JsonObject taskData = task.getAsJsonObject();
                final String id = taskData.get("id").getAsString();
                final String slaveId = taskData.get("slaveId").getAsString();
                System.out.println("ID: " + id + "<==> Slave id: " + slaveId);

                LogFileDownloader filedownloader = LogFileDownloader.builder().slaveId(slaveId).taskId(id).build();
                final String directoryPath = filedownloader.getDirectoryPath();
                System.out.println(directoryPath);

                try {
                    List<String> fileList = filedownloader.getFilesToBeDownloaded(directoryPath);
                    fileList.forEach(path -> {
                        try { //TODO: async download
                            filedownloader.downloadFile("test-001", slaveId, id, path);
                        } catch (IOException | InterruptedException | ExecutionException e) {
                            e.printStackTrace();
                        }
                    });
                } catch (IOException | ExecutionException | InterruptedException e) {
                    e.printStackTrace();
                }
            }));
            //TODO: improve exception handling.
        } catch (IOException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

    }

    private Optional<JsonArray> getTaskInfo(String appId) throws IOException, InterruptedException, ExecutionException {
        String result = null;

        result = client.getURL(MESOS_URL + "/service/marathon/v2/apps/" + appId + "?embed=apps.tasks")
                .get().body().string();
        JsonObject r = new JsonParser().parse(result).getAsJsonObject();

        return Optional.of(r.getAsJsonObject("app")).flatMap(jsonObject ->
                Optional.of(jsonObject.getAsJsonArray("tasks")));
    }
}