package com.francetelecom.dome.producer;

/**
 * User: Eduard.Cojocaru
 * Date: 10/25/13
 */
public class ProducerRunner {

    public static void main(String[] args) {
        new SimpleProducer("my-replicated-topic").start();
    }

}
