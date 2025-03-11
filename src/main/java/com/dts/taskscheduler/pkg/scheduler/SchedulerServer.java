package com.dts.taskscheduler.pkg.scheduler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import javax.sql.DataSource;

import com.dts.taskscheduler.pkg.common.DatabaseConnection;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.*;

class CommandRequest {
    private String command;
    private String scheduledAt;

    public CommandRequest() {
    }

    public CommandRequest(String command, String scheduledAt) {
        this.command = command;
        this.scheduledAt = scheduledAt;
    }

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public String getScheduledAt() {
        return scheduledAt;
    }

    public void setScheduledAt(String scheduledAt) {
        this.scheduledAt = scheduledAt;
    }
}

class StatusResponse {
    private String taskID;
    private String command;
    private String scheduledAt;
    private String pickedAt;
    private String startedAt;
    private String completedAt;
    private String failedAt;

    public String getTaskID() {
        return taskID;
    }

    public void setTaskID(String taskID) {
        this.taskID = taskID;
    }

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public String getScheduledAt() {
        return scheduledAt;
    }

    public void setScheduledAt(String scheduledAt) {
        this.scheduledAt = scheduledAt;
    }

    public String getPickedAt() {
        return pickedAt;
    }

    public void setPickedAt(String pickedAt) {
        this.pickedAt = pickedAt;
    }

    public String getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(String startedAt) {
        this.startedAt = startedAt;
    }

    public String getCompletedAt() {
        return completedAt;
    }

    public void setCompletedAt(String completedAt) {
        this.completedAt = completedAt;
    }

    public String getFailedAt() {
        return failedAt;
    }

    public void setFailedAt(String failedAt) {
        this.failedAt = failedAt;
    }
}

class ResponseObject {
    private String command;
    private long scheduledAt;
    private String taskID;

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public long getScheduledAt() {
        return scheduledAt;
    }

    public void setScheduledAt(long scheduledAt) {
        this.scheduledAt = scheduledAt;
    }

    public String getTaskID() {
        return taskID;
    }

    public void setTaskID(String taskID) {
        this.taskID = taskID;
    }
}

class Task {
    private String id;
    private String command;
    private Instant scheduledAt;
    private Instant pickedAt;
    private Instant startedAt;
    private Instant completedAt;
    private Instant failedAt;

    public Task() {
    }

    public Task(String id, String command, Instant scheduledAt) {
        this.id = id;
        this.command = command;
        this.scheduledAt = scheduledAt;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public Instant getScheduledAt() {
        return scheduledAt;
    }

    public void setScheduledAt(Instant scheduledAt) {
        this.scheduledAt = scheduledAt;
    }

    public Instant getPickedAt() {
        return pickedAt;
    }

    public void setPickedAt(Instant pickedAt) {
        this.pickedAt = pickedAt;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(Instant startedAt) {
        this.startedAt = startedAt;
    }

    public Instant getCompletedAt() {
        return completedAt;
    }

    public void setCompletedAt(Instant completedAt) {
        this.completedAt = completedAt;
    }

    public Instant getFailedAt() {
        return failedAt;
    }

    public void setFailedAt(Instant failedAt) {
        this.failedAt = failedAt;
    }
}

public class SchedulerServer {
    private static final Logger logger = Logger.getLogger(SchedulerServer.class.getName());
    private String serverPort;
    private String dbConnectionString;
    private ExecutorService executor;
    private HttpServer httpServer;
    private DataSource dbPool;
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    public SchedulerServer() {
    }

    public SchedulerServer(String serverPort, String dbConnectionString) {
        this.serverPort = serverPort;
        this.dbConnectionString = dbConnectionString;
        this.executor = Executors.newSingleThreadExecutor();
    }

    public String getServerPort() {
        return serverPort;
    }

    public void setServerPort(String serverPort) {
        this.serverPort = serverPort;
    }

    public String getDbConnectionString() {
        return dbConnectionString;
    }

    public void setDbConnectionString(String dbConnectionString) {
        this.dbConnectionString = dbConnectionString;
    }

    public static SchedulerServer newServer(String port, String dbConnectionString) {
        return new SchedulerServer(
                port,
                dbConnectionString);
    }

    public void start() {
        try {
            dbPool = DatabaseConnection.connectToDatabase(dbConnectionString);
        } catch (Exception e) {
            throw e;
        }

        try {
            httpServer = HttpServer.create(new InetSocketAddress(Integer.parseInt(serverPort)), 0);
            httpServer.createContext("/scheduler", handleScheduleTask());
            httpServer.createContext("/status", handleGetTaskStatus());
        } catch (IOException e) {
            e.printStackTrace();
        }

        logger.info(String.format("Starting scheduler server on %s", serverPort));

        executor.submit(() -> httpServer.start());

        try {
            awaitShutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private HttpHandler handleScheduleTask() {
        return new HttpHandler() {

            @Override
            public void handle(HttpExchange exchange) throws IOException {
                if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                    String errorMessage = "Only POST method is allowed";
                    exchange.sendResponseHeaders(405, errorMessage.getBytes().length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(errorMessage.getBytes());
                    }
                    return;
                }

                ObjectMapper objectMapper = new ObjectMapper();
                CommandRequest commandRequest;
                try (InputStream is = exchange.getRequestBody()) {
                    commandRequest = objectMapper.readValue(is, CommandRequest.class);
                } catch (IOException e) {
                    String errorMessage = e.getMessage();
                    exchange.sendResponseHeaders(400, errorMessage.getBytes().length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(errorMessage.getBytes());
                    }
                    return;
                }
                logger.info("Received schedule request: " + commandRequest);

                Instant scheduledTime;
                try {
                    scheduledTime = Instant.parse(commandRequest.getScheduledAt());
                } catch (Exception e) {
                    String errorMessage = "Invalid date format. Ue ISO 8601 format.";
                    exchange.sendResponseHeaders(400, errorMessage.getBytes().length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(errorMessage.getBytes());
                    }
                    return;
                }
                Instant unixTimestamp = Instant.ofEpochSecond(scheduledTime.getEpochSecond());
                String taskId;
                try {
                    Task task = new Task();
                    task.setCommand(commandRequest.getCommand());
                    task.setScheduledAt(unixTimestamp);
                    taskId = insertTaskIntoDB(task);

                } catch (Exception e) {
                    String errorMessage = "Failed to submit task. Error: " + e.getMessage();
                    exchange.sendResponseHeaders(500, errorMessage.getBytes().length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(errorMessage.getBytes());
                    }
                    return;
                }

                ResponseObject response = new ResponseObject();
                response.setCommand(commandRequest.getCommand());
                response.setScheduledAt(unixTimestamp.getEpochSecond());
                response.setTaskID(taskId);
                System.out.println("came here");

                String jsonResponse;
                try {
                    jsonResponse = objectMapper.writeValueAsString(response);
                } catch (Exception e) {
                    String errorMessage = e.getMessage();
                    exchange.sendResponseHeaders(500, errorMessage.getBytes().length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(errorMessage.getBytes());
                    }
                    return;
                }

                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, jsonResponse.getBytes().length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(jsonResponse.getBytes());
                }
            }
        };
    }

    private String insertTaskIntoDB(Task task) throws Exception {
        String sql = "INSERT INTO tasks (command, scheduled_at) VALUES (?, ?) RETURNING id";
        try (Connection conn = dbPool.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, task.getCommand());
            stmt.setTimestamp(2, Timestamp.from(task.getScheduledAt()));

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("id");
                } else {
                    throw new Exception("Failed to insert task, no ID returned.");
                }
            }
        }
    }

    private HttpHandler handleGetTaskStatus() {
        return exchange -> {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                String errorMessage = "Only GET method is allowed";
                exchange.sendResponseHeaders(405, errorMessage.getBytes().length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(errorMessage.getBytes());
                }
                return;
            }

            String query = exchange.getRequestURI().getQuery();
            UUID taskId = null;

            if (query != null) {
                String[] params = query.split("?");
                for (String param : params) {
                    String[] keyValue = param.split("=");
                    if (keyValue.length == 2 && "task_id".equals(keyValue[0])) {
                        taskId = UUID.fromString(keyValue[1]);
                        break;
                    }
                }
            }

            if (taskId == null) {
                String errorMessage = "Task ID is required";
                exchange.sendResponseHeaders(400, errorMessage.getBytes().length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(errorMessage.getBytes());
                }
                return;
            }

            Task task = null;
            String sql = "SELECT id, command, scheduled_at, picked_at, started_at, completed_at, failed_at " +
                    "FROM tasks WHERE id = ?";

            try (Connection conn = dbPool.getConnection();
                    PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setObject(1, taskId, java.sql.Types.OTHER);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        task = new Task();
                        task.setId(rs.getString("id"));
                        task.setCommand(rs.getString("command"));

                        Timestamp ts = rs.getTimestamp("scheduled_at");
                        if (ts != null) {
                            task.setScheduledAt(ts.toInstant());
                        }

                        ts = rs.getTimestamp("picked_at");
                        if (ts != null) {
                            task.setPickedAt(ts.toInstant());
                        }

                        ts = rs.getTimestamp("started_at");
                        if (ts != null) {
                            task.setStartedAt(ts.toInstant());
                        }

                        ts = rs.getTimestamp("completed_at");
                        if (ts != null) {
                            task.setCompletedAt(ts.toInstant());
                        }

                        ts = rs.getTimestamp("failed_at");
                        if (ts != null) {
                            task.setFailedAt(ts.toInstant());
                        }
                    } else {
                        String errorMessage = "Task not found";
                        exchange.sendResponseHeaders(404, errorMessage.getBytes().length);
                        try (OutputStream os = exchange.getResponseBody()) {
                            os.write(errorMessage.getBytes());
                        }
                        return;
                    }
                }
            } catch (Exception e) {
                String errorMessage = "Failed to get task status. Error: " + e.getMessage();
                exchange.sendResponseHeaders(500, errorMessage.getBytes().length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(errorMessage.getBytes());
                }
                return;
            }

            StatusResponse response = new StatusResponse();
            response.setTaskID(task.getId());
            response.setCommand(task.getCommand());
            response.setScheduledAt(task.getScheduledAt() != null ? task.getScheduledAt().toString() : "");
            response.setPickedAt(task.getPickedAt() != null ? task.getPickedAt().toString() : "");
            response.setStartedAt(task.getStartedAt() != null ? task.getStartedAt().toString() : "");
            response.setCompletedAt(task.getCompletedAt() != null ? task.getCompletedAt().toString() : "");
            response.setFailedAt(task.getFailedAt() != null ? task.getFailedAt().toString() : "");

            ObjectMapper objectMapper = new ObjectMapper();
            String jsonResponse;
            try {
                jsonResponse = objectMapper.writeValueAsString(response);
            } catch (Exception e) {
                String errorMessage = "Failed to marshal JSON response";
                exchange.sendResponseHeaders(500, errorMessage.getBytes().length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(errorMessage.getBytes());
                }
                return;
            }

            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, jsonResponse.getBytes().length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(jsonResponse.getBytes());
            }

        };
    }

    public void awaitShutdown() throws Exception {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown signal received. Releasing latch.");
            this.shutdownLatch.countDown();
        }));

        this.shutdownLatch.await();
        stop();
    }

    public void stop() throws Exception {
        if (dbPool instanceof AutoCloseable) {
            ((AutoCloseable) dbPool).close();
        }

        if (httpServer != null) {
            httpServer.stop(10);
        }

        logger.info("Scheduler server and database pool stopped");
    }

}
