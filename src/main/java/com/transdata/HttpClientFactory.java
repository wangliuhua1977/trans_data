package com.transdata;

import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactoryBuilder;
import org.apache.hc.core5.http.io.SocketConfig;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.hc.core5.ssl.TrustAllStrategy;
import org.apache.hc.core5.util.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;

public class HttpClientFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientFactory.class);

    private final boolean insecure;
    private final Timeout connectTimeout;
    private final Timeout responseTimeout;

    public HttpClientFactory(boolean insecure, Timeout connectTimeout, Timeout responseTimeout) {
        this.insecure = insecure;
        this.connectTimeout = connectTimeout;
        this.responseTimeout = responseTimeout;
    }

    public CloseableHttpClient createClient() {
        PoolingHttpClientConnectionManagerBuilder connectionManagerBuilder = PoolingHttpClientConnectionManagerBuilder.create();
        if (insecure) {
            try {
                SSLContext sslContext = SSLContextBuilder.create()
                        .loadTrustMaterial(TrustAllStrategy.INSTANCE)
                        .build();
                SSLConnectionSocketFactory sslSocketFactory = SSLConnectionSocketFactoryBuilder.create()
                        .setSslContext(sslContext)
                        .setHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                        .build();
                connectionManagerBuilder.setSSLSocketFactory(sslSocketFactory);
                LOGGER.info("HTTPS insecure mode is enabled (trust-all + no hostname verification).");
            } catch (Exception ex) {
                LOGGER.warn("Failed to initialize insecure SSL context: {}", ex.getMessage());
            }
        }

        PoolingHttpClientConnectionManager connectionManager = connectionManagerBuilder.build();
        connectionManager.setDefaultSocketConfig(SocketConfig.custom()
                .setSoTimeout(responseTimeout)
                .build());

        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(connectTimeout)
                .setResponseTimeout(responseTimeout)
                .build();

        return HttpClients.custom()
                .setConnectionManager(connectionManager)
                .setDefaultRequestConfig(requestConfig)
                .build();
    }
}
