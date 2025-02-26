package com.dts.taskscheduler.pkg.model;

import io.grpc.ManagedChannel;

public class WorkerInfoModel {

    public int heartBeatMisses;
    public String address;
    public ManagedChannel grpcConnection;

}
