package com.dts.taskscheduler;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.WebApplicationType;
import com.dts.taskscheduler.pkg.scheduler.SchedulerServer;

@SpringBootApplication
public class TaskschedulerApplication {

	public static void main(String[] args) {
		// Disable the embedded web server (e.g., Tomcat)
		SpringApplication app = new SpringApplication(TaskschedulerApplication.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		app.run(args);

		// Manually start the custom HTTP server for scheduling
		String serverPort = "8081";
		// Replace with your actual database connection string
		String dbConnectionString = "jdbc:postgresql://taskscheduler-postgres-1:5432/dts?user=postgres";
		SchedulerServer schedulerServer = SchedulerServer.newServer(serverPort, dbConnectionString);
		schedulerServer.start();
	}
}
