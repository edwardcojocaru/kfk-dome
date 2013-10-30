package com.francetelecom.dome;

import com.francetelecom.dome.beans.Configuration;
import com.francetelecom.dome.beans.Profile;
import com.francetelecom.dome.beans.Topic;
import com.francetelecom.dome.exception.BadConfigurationException;
import com.francetelecom.dome.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

/**
 * User: eduard.cojocaru Date: 10/29/13
 */
public class ConfigInitializer {

    public static final Logger LOGGER = LoggerFactory.getLogger(ConfigInitializer.class);
    public static final String SPACE = " ";
    public static final String EMPTY = "";
    public static final String COMMA = ",";

    private ResourceBundle resourceBundle;

    public ConfigInitializer() {
        this.resourceBundle = ResourceBundle.getBundle(Constants.CONFIGURATION_FILE_BASE_NAME);
    }

    public Configuration getConfiguration() {

        final Configuration configuration;

        try {

            final int liveCapacity = Integer.parseInt(resourceBundle.getString(Constants.THREADS_NUMBER));
            configuration = new Configuration(getProfiles(liveCapacity), liveCapacity);

        } catch (Exception ex) {
            LOGGER.error("The configuration file might be wrong.", ex);
            throw new BadConfigurationException();
        }

        return configuration;
    }

    private List<Profile> getProfiles(int liveCapacity) {

        final List<Profile> profiles = new ArrayList<>();

        String commaSeparatedTopics = resourceBundle.getString(Constants.TOPICS);
        LOGGER.info("Removing spaces from topics.");
        commaSeparatedTopics = commaSeparatedTopics.replaceAll(SPACE, EMPTY);

        for (String topic : commaSeparatedTopics.split(COMMA)) {
            if (topic != null && !EMPTY.equals(topic)) {
                profiles.add(getProfile(topic));
            }
        }

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Found " + profiles.size() + " topic(s) that will run its jobs in a pool of " + liveCapacity
                    + " thread(s) capacity.");
        }

        return profiles;
    }

    private Profile getProfile(String topic) {

        final String portValue = resourceBundle.getString(topic + Constants.PORT_SUFFIX);
        final String accepted = resourceBundle.getString(topic + Constants.ACCEPT_SUFFIX);
        final String brokers = resourceBundle.getString(topic + Constants.BROKERS_SUFFIX);

        List<Topic> topics = new ArrayList<>();
        topics.add(new Topic(topic, brokers));

        return new Profile(Integer.parseInt(portValue), accepted, topics);
    }
}
