package com.edw.kafka.producer.mbean;

import com.edw.kafka.producer.producer.ApplicationStarter;

/**
 * User: Eduard.Cojocaru
 * Date: 11/11/13
 */
public class ProducerApplication implements ProducerApplicationMBean {

    private ApplicationStarter starter;

    public ProducerApplication(ApplicationStarter starter) {
        this.starter = starter;
    }

    @Override
    public void stopProcessing() {
        starter.stopProducing();
    }
}
