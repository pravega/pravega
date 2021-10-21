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
package io.pravega.cli.user.utils;

import io.jsonwebtoken.lang.Assert;
import org.junit.Test;

public class FormatterTest {

    @Test
    public void testFormatter() {
        Formatter formatter = new Formatter.TableFormatter(new int[]{10, 10});
        Assert.notEmpty(formatter.format("hello=test", "hello2=test2"));
        Assert.isTrue(formatter.separator().contains("-"));

        formatter = new Formatter.JsonFormatter();
        Assert.notEmpty(formatter.format("hello=test"));
        Assert.notEmpty(formatter.format("hello=test", "hello2=test2"));
        Assert.isTrue(formatter.separator().equals(""));
    }
}
