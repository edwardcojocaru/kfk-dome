package com.edw.kafka.producer.exception;

import com.edw.kafka.producer.beans.Profile;

/**
 * User: Eduard.Cojocaru
 * Date: 11/6/13
 */
public class ServerSocketCreationException extends Exception {
    private Profile profile;

    public ServerSocketCreationException(Profile profile, Throwable ex) {
        super(ex);
        this.profile = profile;
    }

    public ServerSocketCreationException(Throwable e) {
        super(e);
    }
}
