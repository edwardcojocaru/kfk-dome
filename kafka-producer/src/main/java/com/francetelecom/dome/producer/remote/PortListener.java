package com.francetelecom.dome.producer.remote;

import com.francetelecom.dome.beans.Profile;
import com.francetelecom.dome.producer.DomeProducer;
import com.francetelecom.dome.producer.ProducerRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Callable;

/**
 * User: eduard.cojocaru
 * Date: 10/29/13
 */
public class PortListener implements Callable {

    private static final Logger LOGGER = LoggerFactory.getLogger(PortListener.class);
    private final ProducerRunner runner;

    private final Profile profile;

    private boolean listening = true;

    public PortListener(Profile profile, ProducerRunner executor) {
        this.profile = profile;
        this.runner = executor;
    }

    public Object call() {

        try (ServerSocket serverSocket = new ServerSocket(profile.getListeningPort())) {
            LOGGER.info("Listening on port: " + profile.getListeningPort());
            while (listening) {
                final Socket clientSocket = serverSocket.accept();
                final InetAddress inetAddress = clientSocket.getInetAddress();

                final boolean isAcceptedListEmpty = !profile.hasAcceptedHosts();

                if (isAcceptedListEmpty || isHostInAcceptedList(inetAddress)) {
                    LOGGER.info("Send data to callable.");
                    runner.submitProducer(getProducer(profile, clientSocket.getInputStream()));
                }
            }

        } catch (IOException e) {
            runner.awaitProducerTermination();
        }

        return null;
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

    public void setListening(boolean listening) {
        this.listening = listening;
    }
}
