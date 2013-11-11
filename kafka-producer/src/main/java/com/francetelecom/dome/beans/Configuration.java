package com.francetelecom.dome.beans;

import com.francetelecom.dome.exception.BadConfigurationException;
import com.francetelecom.dome.util.Constants;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * User: eduard.cojocaru
 * Date: 10/29/13
 */
public class Configuration {

    private List<Profile> profiles;

    private int liveCapacity;

    private String watchedDirectory;
    private Map<String, Object> producerConfig;

    private int managementPort;

    public Configuration(List<Profile> profiles, int liveCapacity) {
        this(profiles, liveCapacity, null, Constants.DEFAULT_MANAGEMENT_PORT, null);
    }

    public Configuration(List<Profile> profiles, int liveCapacity, String watchedDirectory, int managementPort, Map<String, Object> producerConfig) {
        this.profiles = profiles;
        this.liveCapacity = liveCapacity;
        this.watchedDirectory = watchedDirectory;
        this.producerConfig = producerConfig;
        if (profiles == null || profiles.isEmpty() || liveCapacity == 0) {
            throw new BadConfigurationException();
        }

        this.managementPort = managementPort;
    }

    public List<Profile> getProfiles() {
        return Collections.unmodifiableList(profiles);
    }

    public int getLiveCapacity() {
        return liveCapacity;
    }

    public int getListenerCapacity() {
        return profiles.size();
    }

    public String getWatchedDirectory() {
        return watchedDirectory;
    }

    public boolean hasDirectoryToWatch() {
        return watchedDirectory != null && !watchedDirectory.isEmpty();
    }

    public int getManagementPort() {
        return managementPort;
    }

    public Topic getTopic(String fileName) {
        // TODO store a map with prefix vs topic to avoid iteration
        if (fileName != null) {
            for (Profile profile : profiles) {
                for (Topic topic : profile.getTopics()) {
                    if (topic.isValidFilename(fileName)) {
                        return topic;
                    }
                }
            }
        }

        return null;
    }

    public Map<String, Object> getProducerConfig() {
        return producerConfig;
    }
}
