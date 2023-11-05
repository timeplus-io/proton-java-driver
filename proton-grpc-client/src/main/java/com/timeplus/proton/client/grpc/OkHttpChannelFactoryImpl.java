package com.timeplus.proton.client.grpc;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import io.grpc.ManagedChannelBuilder;
import io.grpc.okhttp.OkHttpChannelBuilder;
import com.timeplus.proton.client.ProtonConfig;
import com.timeplus.proton.client.ProtonNode;
import com.timeplus.proton.client.ProtonSslContextProvider;
import com.timeplus.proton.client.grpc.config.ProtonGrpcOption;

final class OkHttpChannelFactoryImpl extends ProtonGrpcChannelFactory {
    private final OkHttpChannelBuilder builder;

    OkHttpChannelFactoryImpl(ProtonConfig config, ProtonNode server) {
        super(config, server);

        builder = OkHttpChannelBuilder.forAddress(server.getHost(), server.getPort());

        int flowControlWindow = (int) config.getOption(ProtonGrpcOption.FLOW_CONTROL_WINDOW);
        if (flowControlWindow > 0) {
            builder.flowControlWindow(flowControlWindow);
        }
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
            try {
                builder.useTransportSecurity().sslSocketFactory(ProtonSslContextProvider.getProvider()
                        .getSslContext(SSLContext.class, config).get().getSocketFactory());
            } catch (SSLException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    @Override
    protected void setupTimeout() {
        // custom socket factory?
    }
}
