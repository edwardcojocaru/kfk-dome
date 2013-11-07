package com.francetelecom.dome.producer.remote;

import com.francetelecom.dome.beans.Profile;
import com.francetelecom.dome.exception.ServerSocketCreationException;
import com.francetelecom.dome.producer.DomeProducer;
import com.francetelecom.dome.producer.ProducerRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * User: eduard.cojocaru
 * Date: 10/29/13
 */
public class PortListener implements Callable<String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PortListener.class);
    private final ProducerRunner runner;

    private final Profile profile;

    private AtomicBoolean listening = new AtomicBoolean(Boolean.TRUE);

    private ServerSocket serverSocket;

    public PortListener(Profile profile, ProducerRunner executor) {
        this.profile = profile;
        this.runner = executor;
    }

    public String call() throws ServerSocketCreationException {

        InetAddress byName = null;
        try {
            byName = InetAddress.getByName(profile.getListeningAddress());
        } catch (UnknownHostException e) {
            try {
                byName = InetAddress.getLocalHost();
            } catch (UnknownHostException e1) {
                LOGGER.error("No address found and localhost not defined.");
            }
        }

        final int listeningPort = profile.getListeningPort();
        try (ServerSocket serverSocket = new ServerSocket(listeningPort, 50, byName)) {
            this.serverSocket = serverSocket;

            LOGGER.info("Listening on port: " + listeningPort);
            while (listening.get()) {

                final Socket clientSocket;
                try {
                    clientSocket = serverSocket.accept();
                } catch (IOException e) {
                    if (listening.get()) {
                        LOGGER.warn("Client may be disconnected. Skipping connection.");
                    }
                    continue;
                }

                final InetAddress inetAddress = clientSocket.getInetAddress();

                final boolean isAcceptedListEmpty = !profile.hasAcceptedHosts();

                if (isAcceptedListEmpty || isHostInAcceptedList(inetAddress)) {
                    LOGGER.info("Send data to callable.");
                    runner.submitProducer(getProducer(profile, clientSocket.getInputStream()));
                }
            }

        } catch (IOException e) {
            LOGGER.error("Socket problem for port {}", listeningPort, e);
            throw new ServerSocketCreationException(profile, e);
        }

        return "ConnectionDoneForPort-" + listeningPort;
    }

    private boolean isHostInAcceptedList(InetAddress inetAddress) {
        return profile.hasAcceptedHosts()
                            && inetAddress != null
                            && profile.getAcceptedHosts().contains(inetAddress.getHostAddress());
    }

    private Callable getProducer(Profile profile, InputStream inputStream) {
        LOGGER.info("Getting dome producer.");
        return new DomeProducer(profile.getTopics().get(0), inputStream);
    }

    public void close() {
        try {
            LOGGER.info("Stoping listener on port {}", profile.getListeningPort());
            listening.set(Boolean.FALSE);
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException e) {
            LOGGER.warn("Socket may be already closed.");
        }
    }
}
