package com.francetelecom.dome.consumer.storm;

import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import com.francetelecom.dome.consumer.KafkaConfigManager;

import java.util.HashMap;
import java.util.Map;

/**
 * User: Eduard.Cojocaru
 * Date: 11/4/13
 */
public class StormTopology {

    public static void main(String[] args) throws Exception {

        final TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("kafkaSpout", new KafkaSpout("real-topic-2p1r"), 3);
        builder.setBolt("SimpleBolt", new SimpleBolt()).shuffleGrouping("kafkaSpout");

        final Map<String, Object> stormConf = new HashMap<>();

        KafkaConfigManager.INSTANCE.init(StormTopology.class.getClassLoader().getResourceAsStream("consumer.properties"));

        stormConf.put("KafkaConfig", KafkaConfigManager.INSTANCE.getConfigurationAsMap());

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("kafkaConsumer", stormConf, builder.createTopology());
    }


}
