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
package io.pravega.shared.rest;

import javax.ws.rs.core.Application;
import java.util.Set;

/**
 * Application to register the REST resource classes.
 */
public class RESTApplication extends Application {
    private final Set<Object> resource;

    public RESTApplication(final Set<Object> resources) {
        super();
        resource = resources;
    }

    /**
     * Get a set of root resources.
     *
     * @return a set of root resource instances.
     */
    @Override
    public Set<Object> getSingletons() {
        return resource;
    }
}
