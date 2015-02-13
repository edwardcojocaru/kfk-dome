package com.edw.kafka.producer.producer.remote;

import com.edw.kafka.producer.beans.Profile;
import com.edw.kafka.producer.exception.ServerSocketCreationException;
import com.edw.kafka.producer.producer.ProducerRunner;
import com.edw.kafka.producer.producer.impl.ProducerContext;
import com.edw.kafka.producer.producer.impl.StreamDomeProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * User: eduard.cojocaru
 * Date: 10/29/13
 */
public class PortListener implements Callable<String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PortListener.class);
    private final ProducerRunner runner;
    private Map<String, Object> producerConfig;

    private final Profile profile;

    private AtomicBoolean listening = new AtomicBoolean(Boolean.TRUE);

    private ServerSocket serverSocket;

    public PortListener(Profile profile, ProducerRunner executor, Map<String, Object> producerConfig) {
        this.profile = profile;
        this.runner = executor;
        this.producerConfig = producerConfig;
    }

    public String call() throws ServerSocketCreationException {

        InetAddress byName = getInetAddress(profile.getListeningAddress());
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

                submitAcceptedProducer(clientSocket);
            }

        } catch (IOException e) {
            LOGGER.error("Socket problem for port {}", listeningPort, e);
            throw new ServerSocketCreationException(profile, e);
        }

        return "ConnectionDoneForPort-" + listeningPort;
    }

    private void submitAcceptedProducer(Socket clientSocket) throws IOException {
        final InetAddress inetAddress = clientSocket.getInetAddress();

        final boolean isAcceptedListEmpty = !profile.hasAcceptedHosts();

        if (isAcceptedListEmpty || isHostInAcceptedList(inetAddress)) {
            LOGGER.info("Send data to callable.");
            runner.submitProducer(getProducer(profile, clientSocket.getInputStream()));
        }
    }

    private InetAddress getInetAddress(String listeningAddress) {
        InetAddress byName = null;
        try {
            byName = InetAddress.getByName(listeningAddress);
        } catch (UnknownHostException e) {
            try {
                byName = InetAddress.getLocalHost();
            } catch (UnknownHostException e1) {
                LOGGER.error("No address found and localhost not defined.");
            }
        }
        return byName;
    }

    private boolean isHostInAcceptedList(InetAddress inetAddress) {
        return profile.hasAcceptedHosts()
                && inetAddress != null
                && profile.getAcceptedHosts().contains(inetAddress.getHostAddress());
    }

    private Callable<Long> getProducer(Profile profile, InputStream inputStream) {
        LOGGER.info("Getting dome producer.");

        final ProducerContext producerContext = new ProducerContext(profile.getTopics().get(0), producerConfig, inputStream);
        return new StreamDomeProducer(producerContext);
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
