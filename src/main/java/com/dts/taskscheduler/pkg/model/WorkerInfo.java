package com.dts.taskscheduler.pkg.model;

import com.dts.taskscheduler.pkg.grpc.WorkerServiceGrpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class WorkerInfo {
    private final int heartbeatMisses;
    private String address;
    private final ManagedChannel grpcConnection;
    private final WorkerServiceGrpc.WorkerServiceBlockingStub workerServiceClient;

    public WorkerInfo(String address, int heartbeatMisses, String host, int port) {
        this.address = address;
        this.heartbeatMisses = heartbeatMisses;
        this.grpcConnection = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        this.workerServiceClient = WorkerServiceGrpc.newBlockingStub(grpcConnection);
    }
}
