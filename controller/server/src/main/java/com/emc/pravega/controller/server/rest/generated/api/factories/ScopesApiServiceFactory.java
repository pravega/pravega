package com.emc.pravega.controller.server.rest.generated.api.factories;

import com.emc.pravega.controller.server.rest.generated.api.ScopesApiService;
import com.emc.pravega.controller.server.rest.generated.api.impl.ScopesApiServiceImpl;

@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-02-15T05:33:34.934-08:00")
public class ScopesApiServiceFactory {
    private final static ScopesApiService service = new ScopesApiServiceImpl();

    public static ScopesApiService getScopesApi() {
        return service;
    }
}
