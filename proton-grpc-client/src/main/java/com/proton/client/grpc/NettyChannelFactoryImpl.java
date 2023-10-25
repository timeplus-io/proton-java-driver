package com.proton.client.grpc;

import java.io.FileNotFoundException;
import javax.net.ssl.SSLException;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.ChannelOption;
import io.grpc.netty.shaded.io.netty.handler.codec.http2.Http2SecurityUtil;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider;
import io.grpc.netty.shaded.io.netty.handler.ssl.SupportedCipherSuiteFilter;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.proton.client.ProtonChecker;
import com.proton.client.ProtonConfig;
import com.proton.client.ProtonNode;
import com.proton.client.ProtonUtils;
import com.proton.client.config.ProtonSslMode;
import com.proton.client.grpc.config.ProtonGrpcOption;

final class NettyChannelFactoryImpl extends ProtonGrpcChannelFactory {
    private final NettyChannelBuilder builder;

    NettyChannelFactoryImpl(ProtonConfig config, ProtonNode server) {
        super(config, server);

        builder = NettyChannelBuilder.forAddress(server.getHost(), server.getPort());

        int flowControlWindow = (int) config.getOption(ProtonGrpcOption.FLOW_CONTROL_WINDOW);
        if (flowControlWindow > 0) {
            builder.flowControlWindow(flowControlWindow); // what about initialFlowControlWindow?
        }
    }

    protected SslContext getSslContext() throws SSLException {
        SslContextBuilder builder = SslContextBuilder.forClient();

        ProtonSslMode sslMode = config.getSslMode();
        if (sslMode == ProtonSslMode.NONE) {
            builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
        } else if (sslMode == ProtonSslMode.STRICT) {
            String sslRootCert = config.getSslRootCert();
            if (!ProtonChecker.isNullOrEmpty(sslRootCert)) {
                try {
                    builder.trustManager(ProtonUtils.getFileInputStream(sslRootCert));
                } catch (FileNotFoundException e) {
                    throw new SSLException("Failed to setup trust manager using given root certificate", e);
                }
            }

            String sslCert = config.getSslCert();
            String sslKey = config.getSslKey();
            if (!ProtonChecker.isNullOrEmpty(sslCert) && !ProtonChecker.isNullOrEmpty(sslKey)) {
                try {
                    builder.keyManager(ProtonUtils.getFileInputStream(sslCert),
                            ProtonUtils.getFileInputStream(sslKey));
                } catch (FileNotFoundException e) {
                    throw new SSLException("Failed to setup key manager using given certificate and key", e);
                }
            }
        }

        builder.sslProvider(SslProvider.JDK).ciphers(Http2SecurityUtil.CIPHERS, SupportedCipherSuiteFilter.INSTANCE);

        return builder.build();
    }

    @Override
    protected ManagedChannelBuilder<?> getChannelBuilder() {
        return builder;
    }

    @Override
    protected void setupSsl() {
        if (!config.isSsl()) {
            builder.usePlaintext();
        } else {
            SslContext sslContext;
            try {
                sslContext = getSslContext();
            } catch (SSLException e) {
                throw new IllegalStateException("Failed to build ssl context", e);
            }

            builder.useTransportSecurity().sslContext(sslContext);
        }
    }

    @Override
    protected void setupTimeout() {
        builder.withOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getConnectionTimeout());
        // .withOption(ChannelOption.SO_TIMEOUT, config.getSocketTimeout());
    }
}
