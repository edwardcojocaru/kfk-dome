package com.francetelecom.dome;

import com.francetelecom.dome.beans.Configuration;
import com.francetelecom.dome.beans.Profile;
import com.francetelecom.dome.beans.Topic;
import com.francetelecom.dome.configuration.ConfigurableFactory;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * User: eduard.cojocaru
 * Date: 10/29/13
 */
public class ConfigInitializerTest {

    @Test
    public void testGetConfiguration() throws Exception {

        final ConfigInitializer configInitializer = new ConfigInitializer(ConfigurableFactory.getConfigurable(null));

        final Configuration configuration = configInitializer.getConfiguration();

        final List<Profile> profiles = configuration.getProfiles();
        assertTrue(profiles.size() > 0);

        final Profile profile = profiles.get(0);
        assertEquals(10001, profile.getListeningPort());
        final List<Topic> topics = profile.getTopics();
        assertTrue(topics.size() > 0);

        final Topic topic = topics.get(0);
        assertEquals("real-topic-5p2r", topic.getName());
        assertEquals("172.16.198.179:9092,172.16.198.179:9093", topic.getBrokerList());
    }
}
