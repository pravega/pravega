/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.embedded;

import com.emc.pravega.controller.server.rpc.v1.ControllerService;
import com.emc.pravega.stream.impl.Controller;

public interface EmbeddedController extends Controller {
    ControllerService getController();
}
