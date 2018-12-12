/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.hash;

import com.google.common.collect.Lists;
import io.pravega.test.common.AssertExtensions;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.function.Supplier;
import lombok.val;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class HashHelperTest {

    @Test
    public void testHuha() {
        HashHelper hasher = HashHelper.seededWith("EventRouter");
        List<String> keys = Lists.newArrayList("0f5c8e82-4a90-4dab-b866-745cac949d72",
        "1203a27d-d5aa-405c-87ba-cc4811b80e82",
                "22cd4044-32a3-4f59-b49b-c467bdc09aaf",
                "37138de7-c701-4042-bc9d-91a1fc52d4be",
                "41f79861-39fe-4231-a980-d84e8a753c5d",
                "452e4ea9-ca70-48a8-9f4b-59a0c7d3030f",
                "4a83d604-83ca-4e27-9119-48d764ef4f2b",
                "7eb437db-631d-49e8-8fd6-46c5bda879d1",
                "8133ce55-0206-4e62-9991-c4c89e4d5822",
                "883e76c3-6556-4751-93d8-08e9a734ab9d",
                "93c9f3ad-a76e-4046-9fe6-63909c303988",
                "aab63967-6ca0-4b15-89f3-e0625a558e54",
                "ad88ceb4-703a-4fc0-9102-4ab6f4d6fab4",
                "b476f659-e8f3-42b1-bea4-47c691c3a6fe",
                "cf45b9fc-3244-4ec8-a26e-298a52866a1c",
                "d6a890a4-051d-4f63-8b22-dade01cc0a6d",
                "f617e1c4-eb5a-4e85-b576-1f0e5c9466bc",
        "f62a2f91-7fe2-4e49-aa54-0b0c1cee4cdf",
        "e7851825-f9b6-4601-bd42-0ce4fad2fb26",
        "e94ce3e6-26d4-48ab-a48a-448aa9bd5203");
        List<Double> hashed = new LinkedList<>();
        keys.forEach(x -> {
            hashed.add(hasher.hashToRange(x));
        });
        hashed.sort(Double::compareTo);
        System.err.println(hashed);
    }
    
    @Test
    public void testLongBits() {
        assertEquals(1.0, HashHelper.longToDoubleFraction(-1), 0.0000001);
        assertEquals(0.0, HashHelper.longToDoubleFraction(0), 0.0000001);
        assertEquals(0.5, HashHelper.longToDoubleFraction(Long.reverse(1)), 0.0000001);
        assertEquals(0.25, HashHelper.longToDoubleFraction(Long.reverse(2)), 0.0000001);
        assertEquals(0.75, HashHelper.longToDoubleFraction(Long.reverse(3)), 0.0000001);
        assertEquals(0.125, HashHelper.longToDoubleFraction(Long.reverse(4)), 0.0000001);
        assertEquals(0.625, HashHelper.longToDoubleFraction(Long.reverse(5)), 0.0000001);
        assertEquals(0.375, HashHelper.longToDoubleFraction(Long.reverse(6)), 0.0000001);
        assertEquals(0.875, HashHelper.longToDoubleFraction(Long.reverse(7)), 0.0000001);
    }

    @Test
    public void testToUUID() {
        UUID uuid = HashHelper.seededWith("Test").toUUID("toUUID");
        assertNotEquals(0, uuid.getMostSignificantBits()); 
        assertNotEquals(0, uuid.getLeastSignificantBits());
        assertNotEquals(Long.MAX_VALUE, uuid.getMostSignificantBits()); 
        assertNotEquals(Long.MAX_VALUE, uuid.getLeastSignificantBits());
        assertNotEquals(-1, uuid.getMostSignificantBits()); 
        assertNotEquals(-1, uuid.getLeastSignificantBits());
        assertNotEquals(Long.MIN_VALUE, uuid.getMostSignificantBits()); 
        assertNotEquals(Long.MIN_VALUE, uuid.getLeastSignificantBits());
    }
    
    @Test
    public void testBytesToUUID() {
        byte[] bytes = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
        UUID uuid = HashHelper.bytesToUUID(bytes);
        assertEquals(uuid.getMostSignificantBits(), 0x0001020304050607L);
        assertEquals(uuid.getLeastSignificantBits(), 0x08090A0B0C0D0E0FL);
    }

    @Test
    public void testHashToBucketString() {
        testBucketUniformity(HashHelper::hashToBucket, () -> RandomStringUtils.randomAlphanumeric(16));
    }

    @Test
    public void testHashToBucketUUID() {
        val r = new Random(0);
        testBucketUniformity(HashHelper::hashToBucket, () -> new UUID(r.nextLong(), r.nextLong()));
    }

    private <T> void testBucketUniformity(HashBucketFunction<T> toTest, Supplier<T> generator) {
        final int elementsPerBucket = 100000;
        final int acceptedDeviation = (int) (0.05 * elementsPerBucket);
        final int bucketCount = 16;
        final int totalCount = bucketCount * elementsPerBucket;
        val h = HashHelper.seededWith("Test");
        val result = new HashMap<Integer, Integer>();
        for (int i = 0; i < totalCount; i++) {
            val u = generator.get();
            int b = toTest.apply(h, u, bucketCount);
            result.put(b, result.getOrDefault(b, 0) + 1);
        }

        result.forEach((bucket, count) ->
                AssertExtensions.assertLessThan("Too many or too few elements in bucket " + bucket,
                        acceptedDeviation, Math.abs(count - elementsPerBucket)));
    }

    @FunctionalInterface
    interface HashBucketFunction<T> {
        int apply(HashHelper hashHelper, T value, int bucketCount);
    }
    
}
