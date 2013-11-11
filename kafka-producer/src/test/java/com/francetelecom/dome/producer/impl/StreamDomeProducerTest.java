package com.francetelecom.dome.producer.impl;

import com.francetelecom.dome.beans.Topic;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.*;

/**
 * User: eduard.cojocaru
 * Date: 10/29/13
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({StreamDomeProducer.class, Topic.class, BufferedReader.class, Producer.class, ProducerConfig.class, KeyedMessage.class})
public class StreamDomeProducerTest {

    private String constantValue;
    private Topic topic;
    private ProducerConfig config;
    private Producer producer;
    private BufferedReader reader;
    private StreamDomeProducer domeProducer;
    private InputStream inputStream;

    @Before
    public void init() throws Exception {
        topic = mock(Topic.class);
        constantValue = "aa";
        when(topic.getAcknowledge()).thenReturn(constantValue);
        when(topic.getBrokerList()).thenReturn(constantValue);
        when(topic.getName()).thenReturn("topic1");

        config = mock(ProducerConfig.class);
        whenNew(ProducerConfig.class).withAnyArguments().thenReturn(config);

        producer = mock(Producer.class);
        whenNew(Producer.class).withArguments(config).thenReturn(producer);

        inputStream = mock(InputStream.class);

        domeProducer = new StreamDomeProducer(topic, inputStream);
    }

    @Test
    public void testConstructor() throws Exception {

        verify(topic).getAcknowledge();
        verify(topic).getBrokerList();
        verify(topic).getName();

        verifyNew(ProducerConfig.class).withArguments(argThat(new ArgumentMatcher<Object>() {
            @Override
            public boolean matches(Object o) {
                if (o instanceof Map) {
                    final Map o1 = (Map) o;

                    return constantValue.equals(o1.get("metadata.broker.list")) && constantValue.equals(o1.get("request.required.acks"));
                }
                return false;
            }
        }));
        verifyNew(Producer.class).withArguments(config);
    }

    @Test
    public void testCall() throws Exception {


        final KeyedMessage keyedMessage = mock(KeyedMessage.class);

        whenNew(KeyedMessage.class).withAnyArguments().thenReturn(keyedMessage);

        final InputStreamReader inputStreamReader = mock(InputStreamReader.class);
        reader = mock(BufferedReader.class);

        whenNew(InputStreamReader.class).withArguments(inputStream).thenReturn(inputStreamReader);
        whenNew(BufferedReader.class).withArguments(inputStreamReader).thenReturn(reader);

        final String line = "line";
        when(reader.readLine()).thenReturn(line).thenReturn(null);


        domeProducer.call();

        verify(topic, times(2)).getName();
        verify(reader, times(2)).readLine();
        verify(producer).send(argThat(new ArgumentMatcher<KeyedMessage>() {
            @Override
            public boolean matches(Object o) {

                final KeyedMessage o1 = (KeyedMessage) o;

                return o1 == keyedMessage;
            }
        }));

        verify(producer).close();

        verifyNew(KeyedMessage.class).withArguments("topic1", line);
        verifyNew(InputStreamReader.class).withArguments(inputStream);
        verifyNew(BufferedReader.class).withArguments(inputStreamReader);
    }

}
