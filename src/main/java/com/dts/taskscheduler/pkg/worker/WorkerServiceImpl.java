package com.dts.taskscheduler.pkg.worker;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import com.dts.taskscheduler.pkg.grpc.CoordinatorServiceGrpc;
import com.dts.taskscheduler.pkg.grpc.Api.HeartbeatRequest;
import com.dts.taskscheduler.pkg.grpc.Api.TaskRequest;
import com.dts.taskscheduler.pkg.grpc.Api.TaskStatus;
import com.dts.taskscheduler.pkg.grpc.Api.UpdateTaskStatusRequest;
import com.dts.taskscheduler.pkg.grpc.WorkerServiceGrpc.WorkerServiceImplBase;
import com.dts.taskscheduler.pkg.model.WorkerServer;

import io.grpc.BindableService;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerBuilder;
import io.grpc.channelz.v1.Server;

public class WorkerServiceImpl extends WorkerServiceImplBase {
    private static final Logger logger = Logger.getLogger(WorkerServer.class.getName());
    private static final Duration DEFAULT_HEARTBEAT = Duration.ofSeconds(10);
    private static final int workerPoolSize = 5;
    private final Duration taskProcessTime = Duration.ofSeconds(5);

    public WorkerServer newServer(String port, String coordinator) {
        return new WorkerServer(
                generateUniqueId(),
                port,
                coordinator,
                DEFAULT_HEARTBEAT,
                new ArrayBlockingQueue<>(100),
                new ConcurrentHashMap<>(),
                new ReentrantLock(),
                Executors.newCachedThreadPool(),
                new CountDownLatch(1));
    }

    private static int generateUniqueId() {
        return Math.abs(UUID.randomUUID().hashCode());
    }

    public void start(WorkerServer server) throws Exception {
        startWorkerPool(server, workerPoolSize);

        try {
            periodicHeartbeat(server);
            startGRPCServer(server);
            awaitShutdown(server);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            closeGRPCConnection(server);
        }
        awaitShutdown(server);
    }

    public boolean connectToCoordinator(WorkerServer server) throws Exception {
        System.out.println("Connecting to coordinator...");

        try {
            server.setGRPCConnection(
                    ManagedChannelBuilder.forTarget(server.getCoordinatorAddress()).usePlaintext().build());
            server.setCoordinatorServiceClient(CoordinatorServiceGrpc.newBlockingStub(server.getGRPCConnection()));
            logger.info("Connected to coordinator!");
            return true;
        } catch (Exception e) {
            logger.severe("Failed to connect to coordinator: " + e.getMessage());
            return false;
        }

    }

    public void periodicHeartbeat(WorkerServer server) {
        server.getActiveTasks().incrementAndGet();

        Runnable heartbeatTask = () -> {
            try {
                if (!sendHeartbeat(server)) {
                    System.out.println("Failed to send heartbeat");
                }
            } catch (Exception e) {
                System.err.println("Exception during heartbeat: " + e.getMessage());
            }
        };

        server.setHeartbeatFuture(server.getScheduler().scheduleAtFixedRate(
                heartbeatTask,
                0,
                server.getHeartbeatInterval().toMillis(),
                TimeUnit.MILLISECONDS));

        server.setHeartbeatFuture(server.getHeartbeatFuture());

        server.getExecutorService().submit(() -> {
            try {
                awaitShutdown(server);
                server.getHeartbeatFuture().cancel(true);
            } catch (Exception e) {
                System.err.println("server shutdown interrupted.");
            }
        });
    }

    public void stop(WorkerServer server) {
        System.out.println("Stopping Worker Server...");

        if (server.getHeartbeatFuture() != null) {
            server.getHeartbeatFuture().cancel(true);
        }

        server.getExecutorService().shutdown();
        try {
            if (!server.getExecutorService().awaitTermination(5, TimeUnit.SECONDS)) {
                server.getExecutorService().shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("ExecutorService shutdown interrupted: " + e.getMessage());
        }

        closeGRPCConnection(server);
        System.out.println("Worker server stopped.");
    }

    public void startWorkerPool(WorkerServer server, int numWorkers) {
        System.out.println("Starting worker pool with " + numWorkers + " workers...");
        ExecutorService executorService = server.getExecutorService();
        for (int i = 0; i < numWorkers; i++) {
            executorService.submit(() -> worker(server));
        }
    }

    public void worker(WorkerServer server) {
        server.getActiveTasks().incrementAndGet();
        ExecutorService executorService = server.getExecutorService();

        while (!server.getCancelToken().get()) {
            try {
                TaskRequest task = server.getTaskQueue().take();
                executorService.submit(() -> updateTaskStatus(server, task, TaskStatus.STARTED));
                processTask(server, task);
                executorService.submit(() -> updateTaskStatus(server, task, TaskStatus.COMPLETED));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        server.getActiveTasks().decrementAndGet();
    }

    public void processTask(WorkerServer server, TaskRequest task) {
        System.out.println("Processing task :" + task);

        try {
            Thread.sleep(taskProcessTime.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Task processing interrupted: " + task);
            return;
        }
        System.out.println("Completed task: " + task);
    }

    public boolean sendHeartbeat(WorkerServer server) throws Exception {
        String workerAddress = System.getenv("WORKER_ADDRESS");
        if (workerAddress == null || workerAddress.isEmpty()) {
            workerAddress = server.getListener().getInetAddress().getHostAddress() + ":"
                    + server.getListener().getLocalPort();
        } else {
            workerAddress += server.getServerPort();
        }

        HeartbeatRequest request = HeartbeatRequest.newBuilder().setWorkerId(server.getId())
                .setAddress(workerAddress).build();
        try {
            CoordinatorServiceGrpc.CoordinatorServiceBlockingStub stub = server.getCoordinatorServiceClient();
            stub.sendHeartbeat(request);
            return true;

        } catch (Exception e) {
            System.err.println("Failed to send heartbeat: " + e.getMessage());
            return false;
        }
    }

    public void startGRPCServer(WorkerServer server) throws Exception {

        if (server.getServerPort() == null || server.getServerPort().isEmpty()) {
            server.setListener(new ServerSocket(0));
            server.setServerPort(":" + server.getListener().getLocalPort());
        } else {
            server.setListener(new ServerSocket());
            server.getListener().bind(new InetSocketAddress(Integer.parseInt(server.getServerPort().replace(":", ""))));
        }

        server.setGrpcServer(ServerBuilder.forPort(Integer.parseInt(server.getServerPort())).addService(this).build());
        server.getGrpcServer().start();

        server.getExecutorService().submit(() -> {
            try {
                server.getGrpcServer().awaitTermination();
            } catch (Exception e) {
                System.err.println("gRPC server interrupted: " + e.getMessage());
            }
        });
    }

    public void awaitShutdown(WorkerServer server) throws Exception {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown signal received. Releasing latch.");
            server.getShutdownLatch().countDown();
        }));
        server.getShutdownLatch().await();
        stop(server);
    }

    public void closeGRPCConnection(WorkerServer server) {
        if (server.getGrpcServer() != null) {
            server.getGrpcServer().shutdown();
            logger.info("gRPC server stopped.");
        }

        if (server.getListener() != null) {
            try {
                server.getListener().close();
            } catch (Exception e) {
                logger.warning("Error while closing the listener: " + e.getMessage());
            }
        }

        if (server.getGRPCConnection() != null) {
            server.getGRPCConnection().shutdown();
            logger.info("Closed client connection with coordinator.");
        }
    }

    public void updateTaskStatus(WorkerServer server, TaskRequest task, TaskStatus status) {
        UpdateTaskStatusRequest request = UpdateTaskStatusRequest.newBuilder().setTaskId(task.getTaskId())
                .setStatus(status).setStartedAt(System.currentTimeMillis() / 1000)
                .setCompletedAt(System.currentTimeMillis() / 1000).build();

        server.getCoordinatorServiceClient().updateTaskStatus(request);
    }

}
