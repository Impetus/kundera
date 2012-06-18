/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/
package com.impetus.kundera.rest.resources;

import javax.persistence.EntityManager;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.rest.common.Constants;
import com.impetus.kundera.rest.repository.EMRepository;

/**
 * REST based resource for JPA query
 * @author amresh
 *
 */

@Path(Constants.KUNDERA_API_PATH + Constants.CRUD_RESOURCE_PATH + "/{sessionToken}")
public class QueryResource
{
    
    /**
     * Handler for GET method requests for this resource
     * Finds an entity from datastore
     * @param sessionToken
     * @param entityClassName
     * @param id
     * @return
     *//*
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
            MetamodelImpl metamodel = (MetamodelImpl)em.getEntityManagerFactory().getMetamodel();
            Class<?> entityClass = metamodel.getEntityClass(entityClassName);
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
    }*/

}
