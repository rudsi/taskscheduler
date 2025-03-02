package com.dts.taskscheduler.pkg.model;

import com.dts.taskscheduler.pkg.grpc.WorkerServiceGrpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class WorkerInfo {
    private int heartbeatMisses;
    private String address;
    private final ManagedChannel grpcConnection;
    private WorkerServiceGrpc.WorkerServiceBlockingStub workerServiceClient;

    public WorkerInfo(String address, int heartbeatMisses, String host, int port) {
        this.address = address;
        this.heartbeatMisses = heartbeatMisses;
        this.grpcConnection = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        this.workerServiceClient = WorkerServiceGrpc.newBlockingStub(grpcConnection);
    }

    public void incrementHeartbeatMisses() {
        this.heartbeatMisses++;
    }

    public int getHeartbeatMisses() {
        return heartbeatMisses;
    }

    public String getAddress() {
        return address;
    }

    public ManagedChannel getGRPCConnection() {
        return grpcConnection;
    }

    public WorkerServiceGrpc.WorkerServiceBlockingStub getWorkerServiceClient() {
        return workerServiceClient;
    }

    public void setWorkerServiceClient(
            WorkerServiceGrpc.WorkerServiceBlockingStub workerServiceClient) {
        this.workerServiceClient = workerServiceClient;
    }

}
