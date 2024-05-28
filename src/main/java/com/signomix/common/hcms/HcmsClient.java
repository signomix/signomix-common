package com.signomix.common.hcms;

import javax.ws.rs.Path;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;

import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

@RegisterRestClient
@Path("/api/docs")
public interface HcmsClient extends AutoCloseable {
    @Produces("application/json")
    Document getDocumentContent(@QueryParam("path") String role) throws ProcessingException, WebApplicationException;

}
