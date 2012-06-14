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

import java.io.IOException;
import java.io.InputStream;

import javax.persistence.EntityManager;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.rest.common.Constants;
import com.impetus.kundera.rest.common.JAXBUtils;
import com.impetus.kundera.rest.common.Response;
import com.impetus.kundera.rest.common.StreamUtils;
import com.impetus.kundera.rest.repository.EMRepository;

/**
 * <Prove description of functionality provided by this Type> 
 * @author amresh.singh
 */

@Path(Constants.KUNDERA_API_PATH + Constants.CRUD_RESOURCE_PATH + "/{sessionToken}/{entityClass}")
public class CRUDResource
{
    /** log for this class. */
    private static Log log = LogFactory.getLog(CRUDResource.class);
    
 
    @POST
    @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @Produces(MediaType.TEXT_PLAIN)
    public String insert(@PathParam("sessionToken") String sessionToken, 
            @PathParam("entityClass") String entityClassName, 
            InputStream in) {      
        
        String inputBody = null;
        try
        {
            inputBody = StreamUtils.toString(in);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        log.debug("POST: SessionToken:" + sessionToken);
        log.debug("POST: entityClass:" + entityClassName);
        log.debug("POST: Input Body:" + inputBody);
        
        EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);
        MetamodelImpl metamodel = (MetamodelImpl)em.getEntityManagerFactory().getMetamodel();
        Class<?> entityClass = metamodel.getEntityClass(entityClassName);
        log.debug("POST: entityClass" + entityClass);
        
        Object entity = JAXBUtils.toObject(inputBody, entityClass);
        em.persist(entity);

        return Response.POST_RESPONSE_SUCCESS;
    }
    
    @GET
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @Path("/{id}")
    public Object find(@PathParam("sessionToken") String sessionToken, 
            @PathParam("entityClass") String entityClassName, @PathParam("id") String id) {
        log.debug("GET: sessionToken:" + sessionToken);
        log.debug("GET: entityClass:" + entityClassName);
        log.debug("GET: ID:" + id);
        EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);
        MetamodelImpl metamodel = (MetamodelImpl)em.getEntityManagerFactory().getMetamodel();
        Class<?> entityClass = metamodel.getEntityClass(entityClassName);
        log.debug("GET: entityClass" + entityClass);
        
        Object entity = em.find(entityClass, id);
        
        log.debug("GET: " + entity);
        return entity;        
    }
    
    @PUT
    @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public Object update(@PathParam("sessionToken") String sessionToken, 
            @PathParam("entityClass") String entityClassName, 
            InputStream in) {      
        
        String xml = null;
        try
        {
            xml = StreamUtils.toString(in);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        log.debug("PUT: sessionToken:" + sessionToken);
        log.debug("PUT: entityClass:" + entityClassName);
        log.debug("PUT: XML:" + xml);
        
        EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);
        MetamodelImpl metamodel = (MetamodelImpl)em.getEntityManagerFactory().getMetamodel();
        Class<?> entityClass = metamodel.getEntityClass(entityClassName);
        log.debug("PUT: entityClass" + entityClass);
        
        Object entity = JAXBUtils.toObject(xml, entityClass);
        Object output = em.merge(entity);
        return output;
    }
    
    @DELETE
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/delete/{id}")
    public String delete(@PathParam("sessionToken") String sessionToken, 
            @PathParam("entityClass") String entityClassName, @PathParam("id") String id) {      
        
        
        log.debug("DELETE: sessionToken:" + sessionToken);
        log.debug("DELETE: entityClass Name:" + entityClassName);
        log.debug("DELETE: ID:" + id);    
        
        
        EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);
        MetamodelImpl metamodel = (MetamodelImpl)em.getEntityManagerFactory().getMetamodel();
        Class<?> entityClass = metamodel.getEntityClass(entityClassName);
        log.debug("DELETE: entityClass" + entityClass);
        
        //Object entity = JAXBUtils.toObject(xml, entityClass);
        Object entity = em.find(entityClass, id);
        em.remove(entity);     
        
        return Response.DELETE_RESPONSE_SUCCESS;
    }

}
