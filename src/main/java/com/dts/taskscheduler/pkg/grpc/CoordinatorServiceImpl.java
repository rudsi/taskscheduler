package com.dts.taskscheduler.pkg.grpc;

import com.dts.taskscheduler.pkg.grpc.Api.ClientTaskRequest;
import com.dts.taskscheduler.pkg.grpc.Api.ClientTaskResponse;
import com.dts.taskscheduler.pkg.grpc.Api.HeartbeatRequest;
import com.dts.taskscheduler.pkg.grpc.Api.HeartbeatResponse;
import com.dts.taskscheduler.pkg.grpc.Api.TaskStatus;
import com.dts.taskscheduler.pkg.grpc.Api.UpdateTaskStatusRequest;
import com.dts.taskscheduler.pkg.grpc.Api.UpdateTaskStatusResponse;

import io.grpc.stub.StreamObserver;

public class CoordinatorServiceImpl extends CoordinatorServiceGrpc.CoordinatorServiceImplBase {

    @Override
    public void submitTask(ClientTaskRequest request, StreamObserver<ClientTaskResponse> responObserver) {
        String taskData = request.getData();
        String taskId = "task-" + System.currentTimeMillis();

        ClientTaskResponse response = ClientTaskResponse.newBuilder().setMessage("Task submitted successfully")
                .setTaskId(taskId).build();

        responObserver.onNext(response);
        responObserver.onCompleted();
    }

    @Override
    public void sendHeartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
        int workerId = request.getWorkerId();
        String address = request.getAddress();

        System.out.println("Heartbeat received from worker " + workerId + " at" + address);

        HeartbeatResponse response = HeartbeatResponse.newBuilder()
                .setAcknowledged(true)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void updateTaskStatus(UpdateTaskStatusRequest request,
            StreamObserver<UpdateTaskStatusResponse> responseObserver) {
        String taskId = request.getTaskId();
        TaskStatus status = request.getStatus();

        System.out.println("Updating task " + taskId + " to status: " + status);

        UpdateTaskStatusResponse response = UpdateTaskStatusResponse.newBuilder()
                .setSuccess(true)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
