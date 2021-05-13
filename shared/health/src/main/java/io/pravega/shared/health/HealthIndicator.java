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
package io.pravega.shared.health;


import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Supplier;

/**
 * The {@link HealthIndicator} interface is the primary interface a client (some arbitrary class) uses to export health
 * information to the {@link HealthService}. At a *minimum* {@link HealthIndicator} is something that *contributes* health {@link Status}
 * information used to determine the well-being of one or more components.
 *
 * A {@link HealthIndicator} will likely want to provide readiness and liveness information of the component.
 */
@Slf4j
public abstract class HealthIndicator implements HealthContributor {

      @Getter
      private final DetailsProvider provider;

      @Getter
      private final String name;

      @NonNull
      public HealthIndicator(String name) {
            this(name, new DetailsProvider());
      }

      @NonNull
      public HealthIndicator(String name, DetailsProvider provider) {
            this.name = name;
            this.provider = provider;
      }

      public Health getHealthSnapshot(boolean includeDetails) {
            Health.HealthBuilder builder = new Health.HealthBuilder();
            try {
                  doHealthCheck(builder);
            } catch (Exception ex) {
                  log.warn("HealthCheck for {} has failed.", this.name, ex);
                  builder.status(Status.DOWN);
            }
            if (includeDetails) {
                  builder.details(this.provider.fetch());
            }
            return builder.name(name).build();
      }

      // Allow an indicator to set a detail dynamically, without exposing the underlying object.
      public void setDetail(String key, Supplier<Object> supplier) {
            provider.add(key, supplier);
      }

      @Override
      public String toString() {
            return String.format("HealthIndicator::%s", this.name);
      }

      /**
       * The {@link HealthIndicator#doHealthCheck(Health.HealthBuilder)} method is the primary interface used by some client
       * to define the logic which determines the health status of a component.
       *
       * This method *must* define logic to assign the {@link Status} that best reflects the current state of the component.
       * - It *should* also determine if the component is considered both {@link Health#isAlive()} and {@link Health#isReady()}.
       *
       * Optionally, {@link DetailsProvider} may be provided to gain further insight to the status of the component. The end result
       * should be a key, value pair of type {@link String}. {@link DetailsProvider} accepts a {@link Supplier}
       * that can return any arbitrary {@link Object}, but said object *must* have the necessary `toString` logic defined
       * (to be human readable).
       *
       * The {@link DetailsProvider} object may be constructed ahead of time and provided during registration
       *
       * @param builder The {@link Health.HealthBuilder} object.
       * @throws Exception An exception to be thrown if the underlying health check fails.
       */
      public abstract void doHealthCheck(Health.HealthBuilder builder) throws Exception;
}
