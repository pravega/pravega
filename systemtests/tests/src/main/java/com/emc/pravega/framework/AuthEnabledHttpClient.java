/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.framework;

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
import java.util.concurrent.CompletableFuture;

import static com.emc.pravega.framework.LoginClient.LOGIN_URL;
import static com.emc.pravega.framework.LoginClient.getAuthenticationRequestInterceptor;

/**
 * Authentication enabled http client.
 */
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
     * Get the HttpClient instance.
     *
     * @return instance of HttpClient.
     */
    public static OkHttpClient getHttpClient() {
        return HttpClientSingleton.INSTANCE.httpClient;
    }

    public static CompletableFuture<Response> getURL(final String url) {

        Request request = new Request.Builder().url(url)
                .header("Authorization", "token=" + LoginClient.getAuthToken(LOGIN_URL,
                        getAuthenticationRequestInterceptor()))
                .build();
        HttpAsyncCallback callBack = new HttpAsyncCallback();
        getHttpClient().newCall(request).enqueue(callBack);
        return callBack.getFuture();
    }

    private static final class HttpAsyncCallback implements Callback {
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

        CompletableFuture<Response> getFuture() {
            return future;
        }
    }
}
