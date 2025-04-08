package com.dts.taskscheduler.pkg.common;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.cdimascio.dotenv.Dotenv;

public class DatabaseConnection {
    private static final Logger logger = Logger.getLogger(DatabaseConnection.class.getName());
    private static final int MAX_RETRIES = 5;
    private static final int RETRY_DELAY_SECONDS = 5;
    private static HikariDataSource dataSource;

    public static DataSource connectToDatabase(String dbConnectionString) {

        if (dbConnectionString == null || dbConnectionString.isEmpty()) {
            throw new IllegalArgumentException("Database connection string must not be null or empty.");
        }
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(dbConnectionString);
        config.setUsername(System.getenv("POSTGRES_USER"));
        config.setPassword(System.getenv("POSTGRES_PASSWORD"));
        config.setMaximumPoolSize(10);
        config.setConnectionTimeout(30000);
        config.setLeakDetectionThreshold(2000);
        config.setIdleTimeout(600000);
        config.setMaxLifetime(1800000);

        int retryCount = 0;
        while (retryCount < MAX_RETRIES) {
            try {
                dataSource = new HikariDataSource(config);
                try (Connection conn = dataSource.getConnection()) {
                    logger.info("Connected to the database.");
                    return dataSource;
                }
                // catch block missing
            } catch (SQLException e) {
                logger.warning("Failed to connect to the database. Retrying in " + RETRY_DELAY_SECONDS + " seconds...");
                try {
                    TimeUnit.SECONDS.sleep(RETRY_DELAY_SECONDS);
                } catch (InterruptedException ignored) {
                    // no handling
                }
                retryCount++;
            }
        }

        throw new RuntimeException("Ran out of retries to connect to database (" + MAX_RETRIES + ")");

    }

    public static String getDBConnectionString() {
        List<String> missingEnvVars = new ArrayList<>();
        Dotenv dotenv = Dotenv.configure().filename(".env").load();

        String dbUser = dotenv.get("POSTGRES_USER");
        if (dbUser == null || dbUser.isEmpty()) {
            missingEnvVars.add("POSTGRES_USER");
        }

        String dbPassword = dotenv.get("POSTGRES_PASSWORD");
        if (dbPassword == null || dbPassword.isEmpty()) {
            missingEnvVars.add("POSTGRES_PASSWORD");
        }

        String dbName = dotenv.get("POSTGRES_DB");
        if (dbName == null || dbName.isEmpty()) {
            missingEnvVars.add("POSTGRES_DB");
        }

        String dbHost = dotenv.get("POSTGRES_HOST");
        if (dbHost == null || dbHost.isEmpty()) {
            dbHost = "localhost";
        }

        if (!missingEnvVars.isEmpty()) {
            String errorMessage = "The following required environment variables are not set: "
                    + String.join(", ", missingEnvVars);
            System.err.println(errorMessage);
            System.exit(1);
        }

        return String.format("jdbc:postgresql://%s:5432/%s?user=%s&password=%s", dbHost, dbName, dbUser, dbPassword);

    }

}
