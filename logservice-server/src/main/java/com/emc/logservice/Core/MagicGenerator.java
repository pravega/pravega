package com.emc.logservice.core;

import java.util.Random;

/**
 * Generates Magic Numbers, which can be used for sequencing elements.
 */
public class MagicGenerator {
    /**
     * Default value for a Magic Number. This indicates no Magic Number has been assigned.
     */
    public static final int NoMagic = Integer.MIN_VALUE;
    private static final Random Generator = new Random();

    /**
     * Generates a new number that is different from NoMagic.
     *
     * @return The newly generated number.
     */
    public static int newMagic() {
        int value;
        do {
            value = Generator.nextInt();
        } while (value == NoMagic);
        return value;
    }
}
