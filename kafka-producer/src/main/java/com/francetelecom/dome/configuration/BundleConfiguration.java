package com.francetelecom.dome.configuration;

import java.util.ResourceBundle;

/**
 * User: Eduard.Cojocaru
 * Date: 11/5/13
 */
public class BundleConfiguration extends AbstractConfiguration {

    private final ResourceBundle bundle;

    public BundleConfiguration(ResourceBundle bundle) {
        this.bundle = bundle;
    }

    @Override
    public Object getProperty(String key) {
        return bundle.getString(key);
    }
}