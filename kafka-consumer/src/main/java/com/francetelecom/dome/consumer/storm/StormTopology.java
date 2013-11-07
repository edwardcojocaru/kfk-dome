package com.francetelecom.dome.consumer.storm;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import com.francetelecom.dome.consumer.configuration.Configurable;
import com.francetelecom.dome.consumer.configuration.ConfigurableFactory;
import com.francetelecom.dome.consumer.utils.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * User: Eduard.Cojocaru
 * Date: 11/4/13
 */
public class StormTopology {

    private static final Logger LOGGER = LoggerFactory.getLogger(StormTopology.class);

    private static final String KAFKA_SPOUT_PREFIX = "kafkaSpout-";
    private static final String BOLT_PREFIX = "SimpleBolt-";
    private static final String SPOUT_PARALLELISM = ".spoutParallelism";

    public static void main(String[] args) throws Exception {

        String configurationPath = null;
        if (args != null && args.length == 1) {
            configurationPath = args[0];
            // TODO in other cases it may throw an exception
        }
        Configurable configurable = ConfigurableFactory.getConfigurable(configurationPath);

        final TopologyBuilder builder = new TopologyBuilder();

        final String topics = configurable.getStringProperty(Constants.TOPICS, "");
        if (topics.isEmpty()) {
            final String message = "'topics' property not specified.";
            LOGGER.info(message);
            System.err.println(message);
            return;
        }

        for (String topic : topics.split(",")) {
            final String spoutId = KAFKA_SPOUT_PREFIX + topic;
            final String boltId = BOLT_PREFIX + topic;
            final int parallelismHint = configurable.getIntProperty(topic + SPOUT_PARALLELISM, Constants.SPOUT_PARALLELISM_DEFAULT_VALUE);

            LOGGER.info("adding kafka spout for '{}'.", topic);
            LOGGER.debug("SpoutId: {}, BoltId: {}", spoutId, boltId);

            builder.setSpout(spoutId, new KafkaSpout(topic), parallelismHint);
            builder.setBolt(boltId, new SimpleBolt()).shuffleGrouping(spoutId);
        }

        final Map<String, Object> stormConfiguration = getStormConfiguration(configurable);
        if (configurable.getBooleanProperty(Constants.CLUSTER_MODE)) {
            LOGGER.info("Starting topology in cluster mode.");
            StormSubmitter.submitTopology("kafkaConsumer", stormConfiguration, builder.createTopology());
        } else {
            LOGGER.info("Starting topology in local mode.");
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("kafkaConsumer", stormConfiguration, builder.createTopology());
        }
    }

    private static Map<String, Object> getStormConfiguration(Configurable configurable) {
        final Map<String, Object> stormConf = new HashMap<>();
        stormConf.put(Constants.KAFKA_CONFIG_KEY, configurable.getPropertiesAsMap());

        LOGGER.debug("Storm config: {}", stormConf);

        return stormConf;
    }


}
