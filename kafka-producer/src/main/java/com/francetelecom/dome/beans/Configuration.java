package com.francetelecom.dome.beans;

import com.francetelecom.dome.exception.BadConfigurationException;

import java.util.Collections;
import java.util.List;

/**
 * User: eduard.cojocaru
 * Date: 10/29/13
 */
public class Configuration {

    private List<Profile> profiles;

    private int liveCapacity;

    private String watchedDirectory;

    public Configuration(List<Profile> profiles, int liveCapacity) {
        this(profiles, liveCapacity, null);
    }

    public Configuration(List<Profile> profiles, int liveCapacity, String watchedDirectory) {
        this.profiles = profiles;
        this.liveCapacity = liveCapacity;
        this.watchedDirectory = watchedDirectory;
        if (profiles == null || profiles.isEmpty() || liveCapacity == 0) {
            throw new BadConfigurationException();
        }
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
}
