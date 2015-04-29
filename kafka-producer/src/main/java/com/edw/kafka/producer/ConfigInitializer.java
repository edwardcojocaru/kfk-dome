package com.edw.kafka.producer;

import com.edw.kafka.producer.beans.Configuration;
import com.edw.kafka.producer.beans.Profile;
import com.edw.kafka.producer.beans.Topic;
import com.edw.kafka.producer.exception.BadConfigurationException;
import com.edw.kafka.producer.util.Constants;
import com.edw.kafka.utils.configuration.Configurable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * User: eduard.cojocaru Date: 10/29/13
 */
public class ConfigInitializer {

    public static final Logger LOGGER = LoggerFactory.getLogger(ConfigInitializer.class);
    public static final String SPACE = " ";
    public static final String EMPTY = "";
    public static final String COMMA = ",";

    private Configurable configurable;

    public ConfigInitializer(Configurable configurable) {
        this.configurable = configurable;
    }

    public Configuration getConfiguration() {

        final Configuration configuration;

        try {

            final int liveCapacity = configurable.getIntProperty(Constants.THREADS_NUMBER, Constants.DEFAULT_THREADS_NUMBER);
            final String watchedDirectory = configurable.getStringProperty(Constants.WATCHED_DIRECTORY);
            final int managementPort = configurable.getIntProperty(Constants.MANAGEMENT_PORT, Constants.DEFAULT_MANAGEMENT_PORT);
            final Map<String, Object> producerConfig = configurable.getConfigProperties(Constants.BASE_PRODUCER_CONFIG);

            configuration = new Configuration(getProfiles(liveCapacity), liveCapacity, watchedDirectory, managementPort, producerConfig);

        } catch (Exception ex) {
            LOGGER.error("The utils file might be wrong.", ex);
            throw new BadConfigurationException();
        }

        return configuration;
    }

    private List<Profile> getProfiles(int liveCapacity) {

        final List<Profile> profiles = new ArrayList<>();

        String commaSeparatedTopics = configurable.getStringProperty(Constants.TOPICS);
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

        final String portValue = configurable.getStringProperty(topic + Constants.PORT_SUFFIX);
        final String accepted = configurable.getStringProperty(topic + Constants.ACCEPT_SUFFIX);
        final String address = configurable.getStringProperty(topic + Constants.ADDRESS_SUFFIX);
        final String brokers = configurable.getStringProperty(topic + Constants.BROKERS_SUFFIX);
        final String filePrefix = configurable.getStringProperty(topic + Constants.TOPIC_FILE_PREFIX);

        List<Topic> topics = new ArrayList<>();
        topics.add(new Topic(topic, brokers, filePrefix));

        return new Profile(Integer.parseInt(portValue), address, accepted, topics);
    }
}
