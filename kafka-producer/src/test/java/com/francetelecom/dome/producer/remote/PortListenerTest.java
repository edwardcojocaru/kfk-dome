package com.francetelecom.dome.producer.remote;

import com.francetelecom.dome.beans.Profile;
import com.francetelecom.dome.beans.Topic;
import com.francetelecom.dome.exception.ServerSocketCreationException;
import com.francetelecom.dome.producer.ProducerRunner;
import com.francetelecom.dome.producer.impl.ProducerContext;
import com.francetelecom.dome.producer.impl.StreamDomeProducer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.verification.ConstructorArgumentsVerification;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;
import static org.junit.Assert.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.*;

/**
 * User: eduard.cojocaru
 * Date: 10/29/13
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({PortListener.class, ProducerRunner.class, ServerSocket.class, InetAddress.class, Socket.class, StreamDomeProducer.class, Profile.class, Topic.class, ProducerContext.class})
public class PortListenerTest {

    @Test
    public void testConstructor() throws Exception {

        final ProducerRunner executor = mock(ProducerRunner.class);
        final Profile profile = mock(Profile.class);

        final PortListener portListener = new PortListener(profile, executor, null);

        assertEquals(profile, getField("profile").get(portListener));
        assertEquals(executor, getField("runner").get(portListener));
        assertEquals(true, ((AtomicBoolean) getField("listening").get(portListener)).get());
        assertNull(getField("serverSocket").get(portListener));
    }

    private Field getField(String fieldName) throws NoSuchFieldException {
        final Field profileField = PortListener.class.getDeclaredField(fieldName);
        profileField.setAccessible(true);
        return profileField;
    }

    @Test
    public void testCall() throws Exception {

        final String listeningAddress = "127.0.0.1";
        final int listeningPort = 1234;

        final ProducerRunner runner = mock(ProducerRunner.class);
        final Profile profile = mock(Profile.class);

        PortListener listener = spy(new PortListener(profile, runner, null));
        final InetAddress localHost = InetAddress.getLocalHost();
        doReturn(localHost).when(listener, "getInetAddress", listeningAddress);
        when(profile.getListeningPort()).thenReturn(listeningPort);
        when(profile.getListeningAddress()).thenReturn(listeningAddress);

        final Constructor<ServerSocket> socketConstructor = ServerSocket.class.getConstructor(int.class, int.class, InetAddress.class);

        final ServerSocket serverSocket = mock(ServerSocket.class);
        whenNew(socketConstructor).withArguments(listeningPort, 50, localHost).thenThrow(new IOException()).thenReturn(serverSocket);

        try {
            listener.call();
            fail("ServerSocketCreationException expected");
        } catch (ServerSocketCreationException e) {

        }

        final Socket socket = mock(Socket.class);
        when(serverSocket.accept()).thenThrow(new IOException()).thenReturn(socket);
        doThrow(new IOException()).when(listener, "submitAcceptedProducer", socket);

        try {
            final String call = listener.call();
            fail("ServerSocketCreationException expected");
        } catch (ServerSocketCreationException e) {

        }

        verifyPrivate(listener, times(2)).invoke("getInetAddress", listeningAddress);
        verify(profile, times(2)).getListeningPort();
        verify(profile, times(2)).getListeningAddress();

        verify(serverSocket, times(2)).accept();
        verifyPrivate(listener).invoke("submitAcceptedProducer", socket);
    }

    @Test
    public void testGetInetAddress() throws Exception {

        final InetAddress inetAddress = mock(InetAddress.class);
        mockStatic(InetAddress.class);
        when(InetAddress.getByName("127.0.0.1")).thenReturn(inetAddress).thenThrow(new UnknownHostException());
        when(InetAddress.getLocalHost()).thenReturn(inetAddress).thenThrow(new UnknownHostException());
        final ProducerRunner executor = mock(ProducerRunner.class);
        final Profile profile = mock(Profile.class);

        final PortListener portListener = new PortListener(profile, executor, null);

        final String getInetAddress = "getInetAddress";
        final Class<?>[] stringClass = new Class<?>[]{String.class};
        final Method getInetAddressMethod = getMethod(portListener, getInetAddress, stringClass);

        assertEquals(inetAddress, getInetAddressMethod.invoke(portListener, "127.0.0.1"));
        assertEquals(inetAddress, getInetAddressMethod.invoke(portListener, "127.0.0.1"));
        assertNull(getInetAddressMethod.invoke(portListener, "127.0.0.1"));

        verifyStatic(times(3));
        InetAddress.getByName("127.0.0.1");
        verifyStatic(times(2));
        InetAddress.getLocalHost();
    }

    private Method getMethod(PortListener portListener, String getInetAddress, Class<?>... stringClass) throws NoSuchMethodException {
        final Method getInetAddressMethod = portListener.getClass().getDeclaredMethod(getInetAddress, stringClass);
        getInetAddressMethod.setAccessible(true);
        return getInetAddressMethod;
    }

    @Test
    public void testGetProducer() throws Exception {

        final ProducerRunner executor = mock(ProducerRunner.class);
        final Profile profile = mock(Profile.class);
        final Topic topic = mock(Topic.class);
        final List<Topic> topics = new ArrayList() {{add(topic);}};
        when(profile.getTopics()).thenReturn(topics);

        final PortListener portListener = new PortListener(profile, executor, null);
        final InputStream inputStream = mock(InputStream.class);

        final Constructor<ProducerContext> producerContextConstructor = ProducerContext.class.getConstructor(Topic.class, Map.class, InputStream.class);
        final ProducerContext producerContext = mock(ProducerContext.class);
        whenNew(producerContextConstructor).withArguments(topic, null, inputStream).thenReturn(producerContext);

        final Constructor<StreamDomeProducer> domeProducerConstructor = StreamDomeProducer.class.getConstructor(ProducerContext.class);
        final StreamDomeProducer domeProducer = mock(StreamDomeProducer.class);
        whenNew(domeProducerConstructor).withArguments(producerContext).thenReturn(domeProducer);

        final Method getProducerMethod = getMethod(portListener, "getProducer", Profile.class, InputStream.class);
        final Object invoke = getProducerMethod.invoke(portListener, profile, inputStream);
        assertEquals(domeProducer, invoke);

        verify(profile).getTopics();

        final ConstructorArgumentsVerification checkProducerContext = verifyNew(ProducerContext.class);
        checkProducerContext.withArguments(topic, null, inputStream);

        final ConstructorArgumentsVerification constructorArgumentsVerification = verifyNew(StreamDomeProducer.class);
        constructorArgumentsVerification.withArguments(producerContext);




    }

    @Test
    public void testClose() throws Exception {

        final ProducerRunner runner = mock(ProducerRunner.class);
        final Profile profile = mock(Profile.class);

        PortListener listener = new PortListener(profile, runner, null);
        ServerSocket serverSocket = mock(ServerSocket.class);

        final Field serverSocketField = getField("serverSocket");
        serverSocketField.set(listener, serverSocket);

        assertTrue(((AtomicBoolean)getField("listening").get(listener)).get());

        listener.close();

        assertFalse(((AtomicBoolean) getField("listening").get(listener)).get());
        verify(serverSocket).close();
    }

    @Test
    public void testCloseSocketException() throws Exception {

        final ProducerRunner runner = mock(ProducerRunner.class);
        final Profile profile = mock(Profile.class);

        PortListener listener = new PortListener(profile, runner, null);
        ServerSocket serverSocket = mock(ServerSocket.class);
        Mockito.doThrow(new IOException()).when(serverSocket).close();

        final Field serverSocketField = getField("serverSocket");
        serverSocketField.set(listener, serverSocket);

        assertTrue(((AtomicBoolean)getField("listening").get(listener)).get());

        listener.close();

        assertFalse(((AtomicBoolean) getField("listening").get(listener)).get());
        verify(serverSocket).close();
    }

    @Test
    public void testCloseNoSocket() throws Exception {

        final ProducerRunner runner = mock(ProducerRunner.class);
        final Profile profile = mock(Profile.class);

        PortListener listener = new PortListener(profile, runner, null);

        assertTrue(((AtomicBoolean)getField("listening").get(listener)).get());

        listener.close();

        assertFalse(((AtomicBoolean) getField("listening").get(listener)).get());
    }

    //
//    private boolean isHostInAcceptedList(InetAddress inetAddress) {
//        return profile.hasAcceptedHosts()
//                && inetAddress != null
//                && profile.getAcceptedHosts().contains(inetAddress.getHostAddress());
//    }
//
}
