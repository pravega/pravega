/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.framework;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller;
import com.emc.pravega.framework.services.RedisService;
import com.emc.pravega.stream.TxnFailedException;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.emc.pravega.framework.LoginClient.LOGIN_URL;
import static com.emc.pravega.framework.LoginClient.MESOS_URL;
import static com.emc.pravega.framework.LoginClient.getAuthenticationRequestInterceptor;
import static com.emc.pravega.framework.TestFrameworkException.Type.RequestFailed;

//Authentication enabled http client
@Slf4j
public class AuthEnabledHttpClient {

    private enum HttpClientSingleton {
        INSTANCE;

        private final OkHttpClient httpClient;

        HttpClientSingleton() {
            httpClient = new OkHttpClient().newBuilder().sslSocketFactory(TrustingSSLSocketFactory.get(),
                    (X509TrustManager) TrustingSSLSocketFactory.get()).hostnameVerifier(new HostnameVerifier() {
                @Override
                public boolean verify(String s, SSLSession sslSession) {
                    return true;
                }
            }).build();
        }
    }

    /**
     * Get the HttpClient
     * @return instance of HttpClient.
     */
    public OkHttpClient getHttpClient() {
        return HttpClientSingleton.INSTANCE.httpClient;
    }

    public CompletableFuture<Response> getURL(final String url) {

        Request request = new Request.Builder().url(url)
                .header("Authorization", "token=" + LoginClient.getAuthToken(LOGIN_URL,
                        getAuthenticationRequestInterceptor()))
                .build();
        HttpAsyncCallback callBack = new HttpAsyncCallback();
            getHttpClient().newCall(request).enqueue(callBack);
        CompletableFuture<Response> future = callBack.getFuture();
        return future;

//            Call call = client.newCall(request).enqueue(callBack);
//            try (Response response = call.execute()) {
//                if (response.isSuccessful()) {
//                    return response.body().string();
//                } else {
//                    throw new TestFrameworkException(TestFrameworkException.Type.RequestFailed,
//                            "Error while performing HTTP(S) GET" + response.message());
//                }
//            }
//        } catch (IOException e) {
//            throw new TestFrameworkException(RequestFailed, "Error while doing httpGet", e);
//        }
    }

    private static final class HttpAsyncCallback implements Callback  {
        private Response result = null;
        private final CompletableFuture<Response> future = new CompletableFuture<>();

        @Override
        public void onFailure(Call call, IOException e) {
            future.completeExceptionally(e);
        }

        @Override
        public void onResponse(Call call, Response response) throws IOException {
            if (!response.isSuccessful()) {
                log.error("Unexpected response. Details: {}", response);
                throw new IOException("Unexpected response code: " + response);
            }
            future.complete(response);
        }

        public CompletableFuture<Response> getFuture() {
            return future;
        }
    }

    public static void main(String[] args) throws Exception {

        RedisService service = new RedisService("redisapp");
        if (!service.isRunning()) {
            service.start(true);
        }

        service.scaleService(2, true);

        System.out.println("hw");
        AuthEnabledHttpClient client = new AuthEnabledHttpClient();

        String result = client.getURL("https://10.240.120.202/service/marathon/v2/apps/redisapp?embed=apps.tasks")
                .get
                ().body().string();
        JsonObject r = new JsonParser().parse(result).getAsJsonObject();

        Optional<JsonArray> r1 = Optional.of(r.getAsJsonObject("app")).flatMap(jsonObject ->
                Optional.of(jsonObject.getAsJsonArray("tasks")));

        r1.ifPresent(tasks -> tasks.forEach(task -> {
            JsonObject taskData = task.getAsJsonObject();

            final String id = taskData.get("id").getAsString();
            final String slaveId = taskData.get("slaveId").getAsString();

            System.out.println("ID: " + id + "<==> Slave id: " + slaveId);
            LogFileDownloader filedownloader = LogFileDownloader.builder().slaveId(slaveId).taskId(id).build();
            final String directoryPath;
            try {
                directoryPath = filedownloader.getDirectoryPath();

                System.out.println(directoryPath);

                List<String> fileList = null;
                try {
                    fileList = filedownloader.getFilesToBeDownloaded(directoryPath);
                } catch (ExecutionException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                fileList.forEach(path -> {
                    try {
                        filedownloader.downloadFile("test-001", slaveId, id, path);
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                });

                System.out.println("hw");
            } catch (IOException e) {
                e.printStackTrace();
            }
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
