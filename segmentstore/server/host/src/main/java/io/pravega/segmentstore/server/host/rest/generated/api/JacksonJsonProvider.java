package io.pravega.segmentstore.server.host.rest.generated.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.util.Json;
import org.glassfish.jersey.jackson.internal.jackson.jaxrs.json.JacksonJaxbJsonProvider;

import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.Provider;

@Provider
@Produces({MediaType.APPLICATION_JSON})
public class JacksonJsonProvider extends JacksonJaxbJsonProvider {
    private static ObjectMapper commonMapper = Json.mapper();

    public JacksonJsonProvider() {
        super.setMapper(commonMapper);
    }
}