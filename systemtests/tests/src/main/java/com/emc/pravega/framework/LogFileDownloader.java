/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.framework;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import lombok.Builder;
import okhttp3.OkHttpClient;
import okhttp3.Request;
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
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.emc.pravega.framework.LoginClient.LOGIN_URL;
import static com.emc.pravega.framework.LoginClient.MESOS_URL;
import static com.emc.pravega.framework.LoginClient.getAuthenticationRequestInterceptor;

@Builder
public class LogFileDownloader {

    private String taskId;
    private String slaveId;

    private final AuthEnabledHttpClient client = new AuthEnabledHttpClient();

    public String getDirectoryPath() {
        String url = "https://10.240.120.202/agent/" + slaveId + "/slave(1)/state";
        //fetch the details of the slave
        String mesosInfo = null;
        try {
            mesosInfo = client.getURL(url).get().body().string();
        } catch (IOException | InterruptedException | ExecutionException e) {
            throw new TestFrameworkException(TestFrameworkException.Type.RequestFailed, "Error while getting slave " +
                    "information", e);
        }
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

    public List<String> getFilesToBeDownloaded(final String directoryPath) throws IOException, ExecutionException,
            InterruptedException {
        //check not null not empty.

        String url = MESOS_URL + "/agent/" + slaveId + "/files/browse?path=" + directoryPath;
        String fileListJson = client.getURL(url).get().body().string();
        JsonArray r1 = new JsonParser().parse(fileListJson).getAsJsonArray();
        List<String> filePaths = new ArrayList<>(5);
        r1.forEach(jsonElement -> filePaths.add(jsonElement.getAsJsonObject().get("path").getAsString()));

        List<String> filteredFilePath = filePaths.stream().filter(s -> s.contains("stderr") || s.contains("stdout")
                || s.contains(".log")).collect(Collectors.toList());
        // fetch the list of files.
        //check if they match the pattern stderr, stdout, .log
        //return the paths as a list
        return filteredFilePath;
    }

    public void downloadFile(final String testName, final String slaveId, final String taskId, final String filePath)
            throws
            IOException, ExecutionException, InterruptedException {
        Path path = Paths.get(MESOS_URL + "/agent/" + slaveId + "/files/download?path=" + filePath);



        Request request = new Request.Builder().url(MESOS_URL + "/agent/" + slaveId + "/files/download?path=" + filePath)
                .header("Authorization", "token=" + LoginClient.getAuthToken(LOGIN_URL,
                        getAuthenticationRequestInterceptor()))
                .build();



        try(Response response = client.getURL(MESOS_URL + "/agent/" + slaveId + "/files/download?path=" + filePath).get()) {
            Path fp = Paths.get(testName, taskId, path.getFileName().toString());
            File file = fp.toFile();
            Files.createDirectories(fp.getParent());
            if( !fp.toFile().exists()) {
                Files.createFile(fp);
            }

            BufferedSink sink = Okio.buffer(Okio.sink(fp.toFile()));
            // you can access body of response
            sink.writeAll(response.body().source());
            sink.close();
        }


        System.out.println("finish");

//        Files.createDirectories(fp.getParent());
//        Files.createFile(fp);


//        InputStream stream = client.getURLAsString(url);

//        IOUtils.copyLarge(stream, new FileOutputStream(Paths.get(taskId, path.getFileName().toString())
//                .toString()));
    }
}
