/**
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.rpc.auth;

import io.pravega.auth.AuthException;
import static io.pravega.auth.AuthHandler.Permissions;
import java.util.List;


/**
 * Helper class for handling auth (authentication and authorization) for the REST API.
 */
public class RESTAuthHelper {

  private AuthHandlerManager authHandlerManager;

  public RESTAuthHelper(AuthHandlerManager authHandlerManager) {
         this.authHandlerManager = authHandlerManager;
  } 

  public void authenticateAuthorize(List<String> authHeader, String resource, Permissions permission)
          throws AuthException {
  }
}
