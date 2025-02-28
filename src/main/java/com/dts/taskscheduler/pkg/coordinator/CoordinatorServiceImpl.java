package com.dts.taskscheduler.pkg.coordinator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.sql.DataSource;

import com.dts.taskscheduler.pkg.common.DatabaseConnection;
import com.dts.taskscheduler.pkg.grpc.Api.ClientTaskRequest;
import com.dts.taskscheduler.pkg.grpc.Api.ClientTaskResponse;
import com.dts.taskscheduler.pkg.grpc.Api.TaskRequest;
import com.dts.taskscheduler.pkg.grpc.Api.TaskResponse;
import com.dts.taskscheduler.pkg.grpc.Api.TaskStatus;
import com.dts.taskscheduler.pkg.grpc.Api.UpdateTaskStatusRequest;
import com.dts.taskscheduler.pkg.grpc.Api.UpdateTaskStatusResponse;
import com.dts.taskscheduler.pkg.grpc.CoordinatorServiceGrpc.CoordinatorServiceImplBase;
import com.dts.taskscheduler.pkg.model.CoordinatorServer;
import com.dts.taskscheduler.pkg.model.WorkerInfo;
import com.zaxxer.hikari.HikariDataSource;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class CoordinatorServiceImpl extends CoordinatorServiceImplBase {
    private final ExecutorService executorService = Executors.newCachedThreadPool();

    @Override
    public void submitTask(ClientTaskRequest request, StreamObserver<ClientTaskResponse> responseObserver) {
        String data = request.getData();
        String taskId = UUID.randomUUID().toString();
        TaskRequest task = TaskRequest.newBuilder().setTaskId(taskId).setData(data).build();
        try {
            submitTaskToWorker(task);
        } catch (Exception e) {
            responseObserver.onError(e);
            return;
        }

        ClientTaskResponse response = ClientTaskResponse.newBuilder().setMessage("Task submitted successfully")
                .setTaskId(taskId).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();

    }

    public CoordinatorServer newServer(String port, String dbConnectionString) {
        int defaultMaxMisses = 3;
        Duration defaultHearbeat = Duration.ofSeconds(5);
        return new CoordinatorServer(port, dbConnectionString, defaultMaxMisses, defaultHearbeat);
    }

    public void start(CoordinatorServer server) throws Exception {
        executorService.submit(() -> {
            try {
                manageWorkerPool();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        try {
            startGRPCServer(server);
        } catch (Exception e) {
            throw new Exception("gRPC server failed to start: " + e.getMessage());
        }

        try {
            server.setDbPool(DatabaseConnection.connectToDatabase(server.getDbConnectionString()));
        } catch (Exception e) {
            throw e;
        }

        executorService.submit(() -> {
            try {
                scanDatabase();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        awaitShutdown(server);
    }

    public void startGRPCServer(CoordinatorServer server) throws Exception {

        System.out.println("Starting gRPC server on " + server.getServerPort());
        server.setGRPCServer(ServerBuilder.forPort(50052).addService(this).build());
        server.getGRPCServer().start();

        executorService.submit(() -> {
            try {
                server.getGRPCServer().awaitTermination();
            } catch (Exception e) {
                System.err.println("gRPC server interrupted: " + e.getMessage());
            }
        });
    }

    public void awaitShutdown(CoordinatorServer server) throws Exception {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown signal received. Releasing latch.");
            server.getShutdownLatch().countDown();
        }));

        server.getShutdownLatch().await();
        stop(server);
    }

    public void stop(CoordinatorServer server) throws Exception {
        executorService.shutdownNow();

        server.getWorkerPoolMutex().lock();

        try {
            for (WorkerInfo worker : server.getWorkerPool().values()) {
                if (worker.getGRPCConnection() != null) {
                    worker.getGRPCConnection().Shutdown();
                }
            }
        } finally {
            server.getWorkerPoolMutex().unlock();
        }

        if (server.getGRPCServer() != null) {
            server.getGRPCServer().shutdown();
            while (!server.getGRPCServer().isTerminated()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        if (server.getListener() != null) {
            server.getListener().close();
        }

        if (server.getDbPool() != null) {
            ((HikariDataSource) server.getDbPool()).close();
        }
    }

    public void updateTaskStatus(CoordinatorServer server, UpdateTaskStatusRequest request,
            StreamObserver<UpdateTaskStatusResponse> responseObserver) throws Exception {
        TaskStatus status = request.getStatus();
        String taskId = request.getTaskId();

        Instant timestamp;
        String column;

        switch (status) {
            case STARTED:
                timestamp = Instant.ofEpochSecond(request.getStartedAt());
                column = "started_at";
                break;
            case COMPLETED:
                timestamp = Instant.ofEpochSecond(request.getCompletedAt());
                column = "completed_at";
                break;
            case FAILED:
                timestamp = Instant.ofEpochSecond(request.getFailedAt());
                column = "failed_at";
                break;
            default:
                System.out.println("Invalid status in UpdateStatusRequest");
                throw new UnsupportedOperationException("Unsupported status: " + status);
        }

        String statement = String.format("UPDATE tasks SET %s = ? WHERE id = ?", column);
        try (Connection conn = server.getDbPool().getConnection();
                PreparedStatement stmt = conn.prepareStatement(statement)) {
            stmt.setTimestamp(1, Timestamp.from(timestamp));
            stmt.setString(2, taskId);

            int rowsUpdated = stmt.executeUpdate();

            if (rowsUpdated == 0) {
                System.out.println("No rows updated for task: " + taskId);
            }
        } catch (SQLException e) {
            System.err.println(String.format("could not update task status for task %s: %s", taskId, e));
            throw e;
        }

        UpdateTaskStatusResponse response = UpdateTaskStatusResponse.newBuilder().setSuccess(true).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public WorkerInfo getNextWorker(CoordinatorServer server) {
        server.getWorkerPoolKeysMutex().readLock().lock();
        try {
            int workerCount = server.getWorkerPoolKeys().size();
            if (workerCount == 0) {
                return null;
            }
            int index = server.getRoundRobinIndex().getAndIncrement() % workerCount;
            int key = server.getWorkerPoolKeys().get(index);
            return server.getWorkerPool().get(key);
        } finally {
            server.getWorkerPoolKeysMutex().readLock().unlock();
        }
    }

    public void submitTaskToWorker(CoordinatorServer server, TaskRequest task) throws Exception {
        WorkerInfo worker = getNextWorker(server);

        if (worker == null) {
            System.out.println("No workers available");
        }

        try {
            worker.workerServiceClient().submitTask(task);
        } catch (StatusRuntimeException e) {
            System.err.println("Error submitting task: " + e.getStatus());
        }

    }
}
