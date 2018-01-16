package io.pravega.segmentstore.server.host.admin.commands;

import com.google.common.base.Preconditions;
import java.util.List;
import lombok.Getter;

/**
 * Command arguments.
 */
@Getter
public class CommandArgs {
    private final List<String> args;
    private final State state;

    /**
     * Creates a new instance of the CommandArgs class.
     *
     * @param args  The actual arguments to the command.
     * @param state The shared State to pass to the command.
     */
    public CommandArgs(List<String> args, State state) {
        this.args = Preconditions.checkNotNull(args, "args");
        this.state = Preconditions.checkNotNull(state, "state");
    }
}
