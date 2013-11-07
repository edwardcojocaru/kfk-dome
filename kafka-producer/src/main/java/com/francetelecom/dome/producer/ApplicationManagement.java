package com.francetelecom.dome.producer;

import com.francetelecom.dome.beans.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * User: Eduard.Cojocaru
 * Date: 11/7/13
 */
public class ApplicationManagement extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationManagement.class);
    private ApplicationStarter applicationStarter;
    private Configuration configuration;

    public ApplicationManagement(ApplicationStarter applicationStarter, Configuration configuration) {
        this.applicationStarter = applicationStarter;
        this.configuration = configuration;
        this.setDaemon(true);
    }

    public void run() {
        final int managementPort = configuration.getManagementPort();
        try (ServerSocket serverSocket = new ServerSocket(managementPort)) {

            LOGGER.info("Listening on management port: " + managementPort);
            boolean listening = true;
            do {
                final Socket clientSocket;
                try {
                    clientSocket = serverSocket.accept();
                } catch (IOException e) {
                    LOGGER.warn("Client may be disconnected. Skipping connection.");
                    continue;
                }
                final InputStream inputStream = clientSocket.getInputStream();
                final PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);

                try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                    final String line = reader.readLine();
                    if (line != null && line.toUpperCase().contains("STOP")) {
                        LOGGER.info("Stopping request received...");
                        out.println("Stopping producer application...");
                        applicationStarter.stopProducing();
                        listening = false;
                    } else {
                        out.println("To stop application try with \"STOP\".");
                    }
                }
            } while (listening);

        } catch (IOException e) {
            LOGGER.error("IOException raised d.", e);
        }
    }
}
