/**
 * Copyright 2012 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera.rest.resources;

import java.io.InputStream;
import java.net.URI;

import javax.persistence.EntityManager;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.rest.common.Constants;
import com.impetus.kundera.rest.common.EntityUtils;
import com.impetus.kundera.rest.common.JAXBUtils;
import com.impetus.kundera.rest.repository.EMRepository;

/**
 * REST resource for CRUD operations
 * @author amresh.singh
 */

@Path(Constants.KUNDERA_API_PATH + Constants.CRUD_RESOURCE_PATH + "/{sessionToken}/{entityClass}")
public class CRUDResource
{
    /** log for this class. */
    private static Log log = LogFactory.getLog(CRUDResource.class);
    
    @Context UriInfo uriInfo;
    
 
    /**
     * Handler for POST method requests for this resource
     * Inserts an entity into datastore
     * @param sessionToken
     * @param entityClassName
     * @param in
     * @return
     */
    @POST
    @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON}) 
    public Response insert(@PathParam("sessionToken") String sessionToken, 
            @PathParam("entityClass") String entityClassName, @Context HttpHeaders headers,
            InputStream in) {      
        
        
        log.debug("POST: SessionToken: " + sessionToken);
        log.debug("POST: entityClass: " + entityClassName);                
        
        Object id;
        try
        {
            EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);
            Class<?> entityClass = EntityUtils.getEntityClass(entityClassName, em);
            log.debug("POST: entityClass" + entityClass);           
            
            String mediaType = headers.getRequestHeader("content-type").get(0);
            log.debug("POST: Media Type:" + mediaType);
            
            Object entity = JAXBUtils.toObject(in, entityClass, mediaType);        
            em.persist(entity);
            
            EntityMetadata m = KunderaMetadataManager.getEntityMetadata(entityClass);
            id = PropertyAccessorHelper.getId(entity, m);
        }
        catch (Exception e)
        {
            log.error(e.getMessage());
            return Response.serverError().build();
        }
        
        return Response.created(URI.create("/" + sessionToken + "/" + entityClassName + "/" + id)).build();
    }
    
    /**
     * Handler for GET method requests for this resource
     * Finds an entity from datastore
     * @param sessionToken
     * @param entityClassName
     * @param id
     * @return
     */
    @GET    
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @Path("/{id}")
    public Response find(@PathParam("sessionToken") String sessionToken, 
            @PathParam("entityClass") String entityClassName, @PathParam("id") String id) {
        
        log.debug("GET: sessionToken:" + sessionToken);
        log.debug("GET: entityClass:" + entityClassName);
        log.debug("GET: ID:" + id);
        
        Object entity = null;
        try
        {
            EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);
            Class<?> entityClass = EntityUtils.getEntityClass(entityClassName, em);
            log.debug("GET: entityClass" + entityClass);
            
            entity = em.find(entityClass, id);
        }
        catch (Exception e)
        {
            log.error(e.getMessage());
            return Response.serverError().build();
        }
        
        log.debug("GET: " + entity);       
        
        if(entity == null) {
            return Response.noContent().build();
        }
        
        return Response.ok(entity).build();              
    }
    
    /**
     * Handler for PUT method requests for this resource
     * Updates an entity into datastore
     * @param sessionToken
     * @param entityClassName
     * @param in
     * @return
     */
    @PUT
    @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public Response update(@PathParam("sessionToken") String sessionToken, 
            @PathParam("entityClass") String entityClassName, @Context HttpHeaders headers,
            InputStream in) {      
        
        
        log.debug("PUT: sessionToken:" + sessionToken);
        log.debug("PUT: entityClassName:" + entityClassName); 
        String mediaType = headers.getRequestHeader("content-type").get(0);
        log.debug("POST: Media Type:" + mediaType);
        
        Object output;
        try
        {
            EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);
            Class<?> entityClass = EntityUtils.getEntityClass(entityClassName, em);
            log.debug("PUT: entityClass: " + entityClass);
            
            
            
            Object entity = JAXBUtils.toObject(in, entityClass, mediaType);
            output = em.merge(entity);
        }
        catch (Exception e)
        {
            log.error(e.getMessage());
            return Response.serverError().build();
        }
        
        if(output == null) {
            return Response.notModified().build();
        }
        
        return Response.ok(output).build();       
    }
    
    /**
     * Handler for DELETE method requests for this resource
     * Deletes an entity from datastore
     * @param sessionToken
     * @param entityClassName
     * @param id
     * @return
     */
    @DELETE
    @Consumes(MediaType.TEXT_PLAIN)    
    @Path("/delete/{id}")
    public Response delete(@PathParam("sessionToken") String sessionToken, 
            @PathParam("entityClass") String entityClassName, @PathParam("id") String id) {      
        
        
        log.debug("DELETE: sessionToken:" + sessionToken);
        log.debug("DELETE: entityClass Name:" + entityClassName);
        log.debug("DELETE: ID:" + id);         
        
        try
        {
            EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);
            Class<?> entityClass = EntityUtils.getEntityClass(entityClassName, em);
            log.debug("DELETE: entityClass" + entityClass);
            
            Object entity = em.find(entityClass, id);
            em.remove(entity);
        }
        catch (Exception e)
        {
            log.error(e.getMessage());
            return Response.serverError().build();
        }
        
        return Response.ok().build();  
        
    }

}
