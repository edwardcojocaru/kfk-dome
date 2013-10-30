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

    public Configuration(List<Profile> profiles, int liveCapacity) {
        this.profiles = profiles;
        this.liveCapacity = liveCapacity;

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
}
