/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.framework;

import com.emc.pravega.framework.services.RedisService;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.util.Optional;

import static com.emc.pravega.framework.LoginClient.MESOS_URL;
import static com.emc.pravega.framework.LoginClient.getAuthenticationRequestInterceptor;
import static com.emc.pravega.framework.TestFrameworkException.Type.RequestFailed;

//Authentication enabled http client
public class AuthEnabledHttpClient {

    private static final String LOGIN_URL = MESOS_URL + "/auth/v1";

    private final OkHttpClient client = new OkHttpClient().newBuilder().sslSocketFactory(TrustingSSLSocketFactory.get(),
            (X509TrustManager) TrustingSSLSocketFactory.get())
            .hostnameVerifier(new HostnameVerifier() {
                @Override
                public boolean verify(String s, SSLSession sslSession) {
                    return true;
                }
            }).build();

    public String getURL(final String url) {

        Request request = new Request.Builder().url(url)
                .header("Authorization", "token=" + LoginClient.getAuthToken(LOGIN_URL,
                        getAuthenticationRequestInterceptor()))
                .build();
        try {
            Call call = client.newCall(request);
            try (Response response = call.execute()) {
                if (response.isSuccessful()) {
                    return response.body().string();
                } else {
                    throw new TestFrameworkException(TestFrameworkException.Type.RequestFailed,
                            "Error while performing HTTP(S) GET" + response.message());
                }
            }
        } catch (IOException e) {
            throw new TestFrameworkException(RequestFailed, "Error while doing httpGet", e);
        }
    }


    public static void main(String[] args) {

        RedisService service = new RedisService("redisapp");
        if (!service.isRunning()) {
            service.start(true);
        }

        service.scaleService(2, true);

        System.out.println("hw");
        AuthEnabledHttpClient client = new AuthEnabledHttpClient();

        String result = client.getURL("https://10.240.124.2/service/marathon/v2/apps/redisapp?embed=apps.tasks");
        JsonObject r = new JsonParser().parse(result).getAsJsonObject();

        Optional<JsonArray> r1 = Optional.of(r.getAsJsonObject("app")).flatMap(jsonObject ->
                Optional.of(jsonObject.getAsJsonArray("tasks")));

        r1.ifPresent(tasks -> tasks.forEach(task -> {
            JsonObject taskData = task.getAsJsonObject();

            final String id = taskData.get("id").getAsString();
            final String slaveId = taskData.get("slaveId").getAsString();

            System.out.println("ID: " + id + "<==> Slave id: " + slaveId);
            LogFileDownloader filedownloader = LogFileDownloader.builder().slaveId(slaveId).taskId(id).build();
            System.out.println(filedownloader.getDirectoryPath());
            System.out.println("hw");

            //
            //            String url = "https://10.240.124.2/agent/" + slaveId + "/slave(1)/state";
            //
            //            String mesosInfo = client.getURL(url);
            //            SlaveState r123 = new Gson().fromJson(mesosInfo, SlaveState.class);
            //
            //            List<String> directoryPaths = new ArrayList<>(2);
            //
            //            r123.getFrameworks().stream()
            //                    .filter(framework -> framework.getName().equals("marathon"))
            //                    .forEach(framework -> {
            //                        //search for task id in the executor.
            //                        framework.getExecutors().stream()
            //                                .filter(executor -> executor.getId().equals(id))
            //                                .forEach(executor -> directoryPaths.add(executor.getDirectory()));
            //                        //Also check the completedExecutors since the service might have crashed and marathon might
            //                        // have spawned a new instance
            //                        framework.getCompleted_executors().stream()
            //                                .filter(executor -> executor.getId().equals(id))
            //                                .forEach(executor -> directoryPaths.add(executor.getDirectory()));
            //                    });

            //            System.out.println("DirectoryPath: " + directoryPaths);

        }));

        System.out.println("hw");

    }
}
