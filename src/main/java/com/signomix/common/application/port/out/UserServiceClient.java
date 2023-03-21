package com.signomix.common.application.port.out;

import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;

import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

import com.signomix.common.User;

@RegisterRestClient
@Path("/api/user")
public interface UserServiceClient extends AutoCloseable{

    @GET
    @Produces("application/json")
    @Path("/{uid}")
    User getUser(@PathParam("uid") String uid, @QueryParam("appkey") String appKey) throws ProcessingException, WebApplicationException;


    @GET
    @Produces("application/json")
    User getUserByNumber(@QueryParam("n") long number, @QueryParam("appkey") String appKey) throws ProcessingException, WebApplicationException;
    
    @GET
    @Produces("application/json")
    List<User> getUsers(@QueryParam("appkey") String appKey, @QueryParam("role") String role) throws ProcessingException, WebApplicationException;

}
