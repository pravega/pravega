/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package io.pravega.test.system.framework;

/**
 * Utility methods used inside the TestFramework.
 */
public class Utils {

    /**
     * Get Configuration from environment or system property.
     * @param key Configuration key
     * @param defaultValue default value incase the property/env is not set
     * @return the configuration value.
     */
    public static String getConfig(final String key, final String defaultValue) {
        return System.getenv().getOrDefault(key, System.getProperty(key, defaultValue));
    }

    /**
     * Helper method to check if skipServiceInstallation flag is set.
     * This flag indicates if the system test framework should reuse services already deployed on the cluster.
     * if set to
     *  true: Already deployed services are used for running tests.
     *  false: Services are deployed on the cluster before running tests.
     *
     * Default value is true
     * @return
     */
    public static boolean isSkipServiceInstallationEnabled() {
        String config = getConfig("skipServiceInstallation", "true");
        return config.trim().equalsIgnoreCase("true") ? true : false;
    }
}
