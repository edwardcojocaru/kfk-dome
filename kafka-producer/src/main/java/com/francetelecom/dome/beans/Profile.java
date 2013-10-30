package com.francetelecom.dome.beans;

import java.util.Collections;
import java.util.List;

/**
 * User: eduard.cojocaru
 * Date: 10/29/13
 */
public class Profile {

    private int listeningPort;

    private String acceptedHosts = "";

    private List<Topic> topics;

    public Profile(int listeningPort, String acceptedHosts, List<Topic> topics) {
        this.listeningPort = listeningPort;
        this.acceptedHosts = acceptedHosts;
        this.topics = topics;
    }

    public int getListeningPort() {
        return listeningPort;
    }

    public String getAcceptedHosts() {
        return acceptedHosts;
    }

    public List<Topic> getTopics() {
        return Collections.unmodifiableList(topics);
    }

    public boolean hasAcceptedHosts() {
        return acceptedHosts != null && !acceptedHosts.isEmpty();
    }
}
