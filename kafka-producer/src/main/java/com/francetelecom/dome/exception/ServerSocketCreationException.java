package com.francetelecom.dome.exception;

import com.francetelecom.dome.beans.Profile;

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
}
