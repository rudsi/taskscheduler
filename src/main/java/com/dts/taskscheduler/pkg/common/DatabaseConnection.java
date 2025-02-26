package com.dts.taskscheduler.pkg.common;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class DatabaseConnection {
    private static final Logger logger = Logger.getLogger(DatabaseConnection.class.getName());
    private static final int MAX_RETRIES = 5;
    private static final int RETRY_DELAY_SECONDS = 5;
    private static HikariDataSource dataSource;

    public static DataSource connectToDatabase(String dbConnectionString) {
        if (dbConnectionString == null || dbConnectionString.isEmpty()) {
            throw new IllegalArgumentException("Database connection string must not be null or empty.");
        }
        HikariConfig config = new HikariDataSource();
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
            } catch (SQLException e) {
                logger.warning("Failed to connect to the database. Retrying in " + RETRY_DELAY_SECONDS + " seconds...");
                try {
                    TimeUnit.SECONDS.sleep(RETRY_DELAY_SECONDS);
                } catch (InterruptedException ignored) {
                }
                retryCount++;
            }
        }

        throw new RuntimeException("Ran out of retries to connect to database (" + MAX_RETRIES + ")");

    }

}
