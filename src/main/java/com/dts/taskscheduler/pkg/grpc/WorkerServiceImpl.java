package com.dts.taskscheduler.pkg.grpc;

import com.dts.taskscheduler.pkg.grpc.Api.TaskRequest;
import com.dts.taskscheduler.pkg.grpc.Api.TaskResponse;

import io.grpc.stub.StreamObserver;

public class WorkerServiceImpl extends WorkerServiceGrpc.WorkerServiceImplBase {

    @Override
    public void submitTask(TaskRequest request, StreamObserver<TaskResponse> responseObserver) {
        String taskId = request.getTaskId();
        String data = request.getData();

        System.out.println("Received task: " + taskId + " with data: " + data);
        TaskResponse response = TaskResponse.newBuilder().setTaskId(taskId).setMessage(("Task received successfully"))
                .setSuccess(true).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

}
