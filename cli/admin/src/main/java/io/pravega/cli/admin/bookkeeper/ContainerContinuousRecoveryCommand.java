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
package io.pravega.cli.admin.bookkeeper;

import io.pravega.cli.admin.CommandArgs;
import io.pravega.common.util.ReusableLatch;
import io.pravega.segmentstore.storage.DurableDataLogException;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ContainerContinuousRecoveryCommand extends ContainerRecoverCommand {

    /**
     * Creates a new instance of the ContainerContinuousRecoveryCommand.
     *
     * @param args The arguments for the command.
     */
    public ContainerContinuousRecoveryCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() throws Exception {
        ensureArgCount(2);
        int numberOfRuns = getIntArg(0);
        int secondsBetweenRuns = getIntArg(1);
        AtomicInteger counter = new AtomicInteger(0);
        AtomicBoolean failed = new AtomicBoolean(false);
        ReusableLatch latch = new ReusableLatch();
        Runnable containerRecoveryIteration = () -> {
            // If we reach the desired number of iterations or a failure has happened, release the latch and return.
            if (counter.get() >= numberOfRuns || failed.get()) {
                output("Finishing recovery run iterations (failed? " + failed.get() + ")");
                latch.release();
                return;
            }
            // Recover all the Segment Containers sequentially.
            for (int i = 0; i < getServiceConfig().getContainerCount(); i++) {
                try {
                    super.performRecovery(i);
                    counter.getAndIncrement();
                } catch (DurableDataLogException e) {
                    // We found an error while recovering a container. Stop running further recoveries to debug this one.
                    output("Problem recovering container " + i + ", terminating execution of command.");
                    failed.set(true);
                    e.printStackTrace();
                    break;
                }
            }
        };
        ScheduledFuture<?> future = getCommandArgs().getState().getExecutor().scheduleAtFixedRate(containerRecoveryIteration,
                secondsBetweenRuns, secondsBetweenRuns, TimeUnit.SECONDS);
        latch.await();
        future.cancel(true);
        output("Completed continuous Segment Container recovery command.");
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "continuous-recover",
                "Executes a local, non-invasive recovery for all SegmentContainers in the cluster during the specified duration.",
                new ArgDescriptor("number-of-runs", "Number of iterations to recover all Segment Containers."),
                new ArgDescriptor("seconds-between-runs", "Seconds to wait between execution iterations."));
    }
}
