module com.proton.client.grpc {
    exports com.proton.client.grpc;
    exports com.proton.client.grpc.config;
    exports com.proton.client.grpc.impl;

    provides com.proton.client.ProtonClient with com.proton.client.grpc.ProtonGrpcClient;

    requires transitive com.proton.client;
    requires transitive com.google.gson;
    requires transitive com.google.protobuf;
    requires transitive io.grpc;
    // requires transitive grpc.core;
    // requires transitive grpc.protobuf;
    // requires transitive grpc.stub;
}
