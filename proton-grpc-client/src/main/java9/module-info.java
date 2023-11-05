module com.timeplus.proton.client.grpc {
    exports com.timeplus.proton.client.grpc;
    exports com.timeplus.proton.client.grpc.config;
    exports com.timeplus.proton.client.grpc.impl;

    provides com.timeplus.proton.client.ProtonClient with com.timeplus.proton.client.grpc.ProtonGrpcClient;

    requires transitive com.timeplus.proton.client;
    requires transitive com.google.gson;
    requires transitive com.google.protobuf;
    requires transitive io.grpc;
    // requires transitive grpc.core;
    // requires transitive grpc.protobuf;
    // requires transitive grpc.stub;
}
