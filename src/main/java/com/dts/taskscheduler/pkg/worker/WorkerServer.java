package com.dts.taskscheduler.pkg.worker;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import com.dts.taskscheduler.pkg.grpc.CoordinatorServiceGrpc;
import com.dts.taskscheduler.pkg.grpc.Api.ClientTaskRequest;
import com.dts.taskscheduler.pkg.grpc.Api.ClientTaskResponse;
import com.dts.taskscheduler.pkg.grpc.Api.HeartbeatRequest;
import com.dts.taskscheduler.pkg.grpc.Api.TaskRequest;
import com.dts.taskscheduler.pkg.grpc.Api.TaskResponse;
import com.dts.taskscheduler.pkg.grpc.Api.TaskStatus;
import com.dts.taskscheduler.pkg.grpc.Api.UpdateTaskStatusRequest;
import com.dts.taskscheduler.pkg.grpc.WorkerServiceGrpc.WorkerServiceImplBase;

import io.github.cdimascio.dotenv.Dotenv;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

public class WorkerServer extends WorkerServiceImplBase {
    private static final Logger logger = Logger.getLogger(WorkerServer.class.getName());
    private static final Duration DEFAULT_HEARTBEAT = Duration.ofSeconds(10);
    private static final int workerPoolSize = 5;
    private final Duration taskProcessTime = Duration.ofSeconds(5);

    private final int id;
    private String serverPort;
    private final String coordinatorAddress;
    private ServerSocket listener;
    private Server grpcServer;
    private ManagedChannel grpcConnection;
    private CoordinatorServiceGrpc.CoordinatorServiceBlockingStub coordinatorServiceClient;
    private final Duration heartbeatInterval;
    private BlockingQueue<TaskRequest> taskQueue;
    private final ExecutorService executorService;
    private final AtomicInteger activeTasks = new AtomicInteger(0);
    private ScheduledFuture<?> heartbeatFuture;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final AtomicBoolean cancelToken = new AtomicBoolean(false);
    private final CountDownLatch shutdownLatch;
    private Map<String, TaskRequest> receivedTasks;
    private final ReentrantLock receivedTasksMutex;

    Dotenv dotenv = Dotenv.configure().filename(".env").load();

    public WorkerServer(int id, String serverPort, String coordinatorAddress, Duration heartbeatInterval,
            BlockingQueue<TaskRequest> taskQueue, Map<String, TaskRequest> receivedTasks,
            ReentrantLock receivedTasksMutex, ExecutorService executorService, CountDownLatch shutDownLatch) {
        this.id = id;
        this.serverPort = serverPort;
        if (serverPort == null || serverPort.isEmpty()) {
            try {
                this.listener = new ServerSocket(0);
            } catch (IOException e) {
                System.err.println("Error initializing ServerSocket: " + e.getMessage());
            }
        } else {
            this.listener = null;
        }

        this.coordinatorAddress = coordinatorAddress;
        this.grpcConnection = ManagedChannelBuilder.forTarget(coordinatorAddress).usePlaintext().build();
        this.heartbeatInterval = heartbeatInterval;
        this.taskQueue = taskQueue;
        this.receivedTasks = receivedTasks;
        this.receivedTasksMutex = receivedTasksMutex;
        this.executorService = executorService;
        this.shutdownLatch = shutDownLatch;
        this.coordinatorServiceClient = CoordinatorServiceGrpc.newBlockingStub(grpcConnection);
    }

    public void submitTask(TaskRequest request, StreamObserver<TaskResponse> responseObserver) {
        receivedTasksMutex.lock();
        try {
            receivedTasks.put(request.getTaskId(), request);
        } finally {
            receivedTasksMutex.unlock();
        }
        taskQueue.offer(request);
        TaskResponse response = TaskResponse.newBuilder().setMessage("Task was submitted").setSuccess(true)
                .setTaskId(request.getTaskId()).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static WorkerServer newServer(String port, String coordinator) {
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

    public void start() throws Exception {
        startWorkerPool(workerPoolSize);
        try {
            connectToCoordinator();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        try {
            startGRPCServer();
        } catch (Exception e) {
            System.err.println("Error starting gRPC server: " + e.getMessage());
            throw e;
        }
        periodicHeartbeat();
        awaitShutdown();
        stop();
    }

    public boolean connectToCoordinator() throws Exception {
        System.out.println("Connecting to coordinator...");

        try {
            grpcConnection = ManagedChannelBuilder.forTarget(coordinatorAddress)
                    .usePlaintext()
                    .build();
            grpcConnection.getState(true);

            coordinatorServiceClient = CoordinatorServiceGrpc.newBlockingStub(grpcConnection);
            logger.info("Connected to coordinator!");
            return true;
        } catch (Exception e) {
            logger.severe("Failed to connect to coordinator: " + e.getMessage());
            throw e;
        }
    }

    public void periodicHeartbeat() {
        activeTasks.incrementAndGet();

        Runnable heartbeatTask = () -> {
            try {
                sendHeartbeat();
            } catch (Exception e) {
                System.err.println("Exception during heartbeat: " + e.getMessage());
            }
        };

        heartbeatFuture = scheduler.scheduleAtFixedRate(
                heartbeatTask,
                0,
                heartbeatInterval.toMillis(),
                TimeUnit.MILLISECONDS);
    }

    public void stop() {
        System.out.println("Stopping Worker Server...");

        if (heartbeatFuture != null) {
            heartbeatFuture.cancel(true);
        }

        scheduler.shutdown();
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("ExecutorService shutdown interrupted: " + e.getMessage());
        }
        if (grpcServer != null) {
            grpcServer.shutdown();
            System.out.println("grpc server stopped from inside the stop method");
        }

        closeGRPCConnection();
        System.out.println("Worker server stopped.");
    }

    public void startWorkerPool(int numWorkers) {
        System.out.println("Starting worker pool with " + numWorkers + " workers...");
        for (int i = 0; i < numWorkers; i++) {
            executorService.submit(() -> worker());
        }
    }

    public void worker() {
        activeTasks.incrementAndGet();

        while (!cancelToken.get()) {
            try {
                TaskRequest task = taskQueue.take();
                executorService.submit(() -> updateTaskStatus(task, TaskStatus.STARTED));
                processTask(task);
                executorService.submit(() -> updateTaskStatus(task, TaskStatus.COMPLETED));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        activeTasks.decrementAndGet();
    }

    public void processTask(TaskRequest task) {
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

    public void sendHeartbeat() throws Exception {
        String workerAddress = dotenv.get("WORKER_ADDRESS");
        if (workerAddress == null || workerAddress.isEmpty()) {
            if (listener != null && !listener.isClosed()) {
                workerAddress = listener.getInetAddress().getHostAddress() + ":"
                        + listener.getLocalPort();
            } else {
                workerAddress = "localhost:" + serverPort;
            }

        } else {
            workerAddress += serverPort;
            System.out.println("server port is being used." + workerAddress);
        }

        HeartbeatRequest request = HeartbeatRequest.newBuilder().setWorkerId(id)
                .setAddress("localhost:8082").build();
        try {
            System.out.println("Sending heartbeat request: " + request);
            coordinatorServiceClient.sendHeartbeat(request);
            System.out.println("Heartbeat request sent successfully.");

        } catch (Exception e) {
            System.err.println("Failed to send heartbeat: " + e.getMessage());
        }
    }

    public void startGRPCServer() throws Exception {

        grpcServer = ServerBuilder.forPort(8082).addService(this).build();
        grpcServer.start();

        executorService.submit(() -> {
            try {
                grpcServer.awaitTermination();
            } catch (Exception e) {
                System.err.println("gRPC server interrupted: " + e.getMessage());
            }
        });
    }

    public void awaitShutdown() throws Exception {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown signal received. Releasing latch.");
            shutdownLatch.countDown();
        }));
        shutdownLatch.await();
        stop();
    }

    public void closeGRPCConnection() {
        if (grpcServer != null) {
            grpcServer.shutdown();
            logger.info("gRPC server stopped.");
        }

        if (listener != null) {
            try {
                listener.close();
            } catch (Exception e) {
                logger.warning("Error while closing the listener: " + e.getMessage());
            }
        }

        if (grpcConnection != null) {
            grpcConnection.shutdown();
            logger.info("Closed client connection with coordinator.");
        }
    }

    public void updateTaskStatus(TaskRequest task, TaskStatus status) {
        UpdateTaskStatusRequest request = UpdateTaskStatusRequest.newBuilder().setTaskId(task.getTaskId())
                .setStatus(status).setStartedAt(System.currentTimeMillis() / 1000)
                .setCompletedAt(System.currentTimeMillis() / 1000).build();

        coordinatorServiceClient.updateTaskStatus(request);
    }

}
