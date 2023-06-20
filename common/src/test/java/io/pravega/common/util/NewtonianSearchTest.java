/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pravega.common.util;
import org.junit.Test;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class NewtonianSearchTest {
    @Test
    public void testNewtonianSearch_WithExistingKey() {
        List<Integer> numbers = getNumbersList();
        Integer target = 9;
        NewtonianSearch.DensityFunction<Integer> densityFunction = number -> number;
        int resultIndex = NewtonianSearch.newtonianSearch(numbers, densityFunction, target);
        assertEquals(4, resultIndex);
    }

    @Test
    public void testNewtonianSearch_WithNonExistingKey() {
        List<Integer> numbers = getNumbersList();
        int target = 8;
        NewtonianSearch.DensityFunction<Integer> densityFunction = number -> number;
        int resultIndex = NewtonianSearch.newtonianSearch(numbers, densityFunction, target);
        assertEquals(-5, resultIndex);
    }

    @Test
    public void testNewtonianSearch_WithEmptyList() {
        List<Integer> numbers = new ArrayList<>();
        int target = 5;
        NewtonianSearch.DensityFunction<Integer> densityFunction = number -> number;
        int resultIndex = NewtonianSearch.newtonianSearch(numbers, densityFunction, target);
        assertEquals(-1, resultIndex);
    }

    @Test
    public void testNewtonianSearch_WithStrings() {
        List<String> words = new ArrayList<>();
        words.add("apple");
        words.add("banana");
        words.add("carrot");
        words.add("dry-date");
        words.add("elderberry");
        String target = "carrot";
        NewtonianSearch.DensityFunction<String> densityFunction = word -> word.length();
        int resultIndex = NewtonianSearch.newtonianSearch(words, densityFunction, target);
        assertEquals(2, resultIndex);
    }

    public static List getNumbersList() {
        List<Integer> numbers = new ArrayList<>();
        numbers.add(1);
        numbers.add(3);
        numbers.add(5);
        numbers.add(7);
        numbers.add(9);
        numbers.add(11);
        numbers.add(13);
        numbers.add(15);
        numbers.add(17);
        numbers.add(19);
        return numbers;
    }
}
