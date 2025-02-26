package com.dts.taskscheduler.pkg.model;

import com.dts.taskscheduler.pkg.grpc.WorkerServiceGrpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class WorkerInfo {
    private int heartbeatMisses;
    private String address;
    private ManagedChannel grpcConnection;
    private WorkerServiceGrpc.WorkerServiceBlockingStub workerServiceClient;

    public WorkerInfo(String address, ManagedChannel grpcConnection,
            WorkerServiceGrpc.WorkerServiceBlockingStub workerServiceClient) {
        this.heartbeatMisses = 0;
        this.address = address;
        this.grpcConnection = ManagedChannelBuilder.forTarget(address)
                .usePlaintext()
                .build();
        this.workerServiceClient = WorkerServiceGrpc.newBlockingStub(grpcConnection);
    }

    public int getHeartbeatMisses() {
        return heartbeatMisses;
    }

    public void incrementHeartbeatMisses() {
        this.heartbeatMisses++;
    }

    public void resetHeartbeatMisses() {
        this.heartbeatMisses = 0;
    }

    public String getAddress() {
        return address;
    }

    public ManagedChannel getGrpcConnection() {
        return grpcConnection;
    }

    public WorkerServiceGrpc.WorkerServiceBlockingStub getWorkerServiceClient() {
        return workerServiceClient;
    }

    public void shutdownConnection() {
        if (grpcConnection != null) {
            grpcConnection.shutdown();
        }
    }
}
