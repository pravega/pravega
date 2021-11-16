package io.pravega.shared.health;

/**
 * This HealthConnector interface provides a single method definition to help define how some class may attach/register
 * its child fields under the parent HealthContributor (i.e. make it reachable from the {@link HealthServiceManager}).
 *
 * The benefit of isolating this one method is to prevent having to change the constructors of all intermediate classes
 * to include some {@link HealthContributor} object. Instead, a class that does not directly instantiate its own
 * {@link HealthContributor} can forward the register call to the appropriate child objects.
 */
public interface HealthConnector {

    private HealthContributor getHealthContributor() {
        return null;
    }

    /**
     * Registers any number of {@link HealthContributor} as children to referenced {@link HealthContributor}.
     * @param parent The parent {@link HealthContributor}.
     */
    default void connect(HealthContributor parent) {
        HealthContributor contributor = getHealthContributor();
        if (contributor != null) {
            parent.register(contributor);
        }
    }

    /**
     * This call should be used as a pass-through method, where the class implementing this method does not have any
     * {@link HealthContributor} objects itself to register, but it has some child objects which may contain some
     * {@link HealthContributor} objects.
     * @param parent
     */
    default void connect(HealthConnector parent) {

    }
}
