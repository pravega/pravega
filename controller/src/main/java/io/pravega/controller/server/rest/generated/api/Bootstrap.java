package io.pravega.controller.server.rest.generated.api;

import io.swagger.jaxrs.config.SwaggerContextService;
import io.swagger.models.*;

import io.swagger.models.auth.*;

import javax.servlet.http.HttpServlet;
import javax.servlet.ServletContext;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;

@SuppressWarnings("all")
public class Bootstrap extends HttpServlet {
  @Override
  public void init(ServletConfig config) throws ServletException {
    Info info = new Info()
      .title("Swagger Server")
      .description("List of admin REST APIs for the Pravega controller service.")
      .termsOfService("")
      .contact(new Contact()
        .email(""))
      .license(new License()
        .name("Apache 2.0")
        .url("http://www.apache.org/licenses/LICENSE-2.0"));

    ServletContext context = config.getServletContext();
    Swagger swagger = new Swagger().info(info);

    new SwaggerContextService().withServletConfig(config).updateSwagger(swagger);
  }
}
