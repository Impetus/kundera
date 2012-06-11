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

import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.rest.common.JAXBUtils;
import com.impetus.kundera.rest.common.StreamUtils;
import com.impetus.kundera.rest.repository.EMRepository;

/**
 * <Prove description of functionality provided by this Type> 
 * @author amresh.singh
 */

@Path("/kundera/api/crud/{sessionToken}/{entityClass}")
public class CRUDResource
{
    
 
    @POST
    @Consumes(MediaType.APPLICATION_XML)
    @Produces(MediaType.TEXT_PLAIN)
    public String insert(@PathParam("sessionToken") String sessionToken, 
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
        System.out.println("sessionToken:" + sessionToken);
        System.out.println("entityClass:" + entityClassName);
        System.out.println(xml);
        
        EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);
        MetamodelImpl metamodel = (MetamodelImpl)em.getEntityManagerFactory().getMetamodel();
        Class<?> entityClass = metamodel.getEntityClass(entityClassName);
        System.out.println("entityClass" + entityClass);
        
        Object entity = JAXBUtils.toObject(xml, entityClass);
        em.persist(entity);

        return "Data saved successfully";
    }
    
    @GET
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.APPLICATION_XML)
    @Path("/{id}")
    public Object find(@PathParam("sessionToken") String sessionToken, 
            @PathParam("entityClass") String entityClassName, @PathParam("id") String id) {
        System.out.println("sessionToken:" + sessionToken);
        System.out.println("entityClass:" + entityClassName);
        System.out.println("ID:" + id);
        EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);
        MetamodelImpl metamodel = (MetamodelImpl)em.getEntityManagerFactory().getMetamodel();
        Class<?> entityClass = metamodel.getEntityClass(entityClassName);
        System.out.println("entityClass" + entityClass);
        
        Object entity = em.find(entityClass, id);
        
        System.out.println(entity);
        return entity;        
    }
    
    @PUT
    @Consumes(MediaType.APPLICATION_XML)
    @Produces(MediaType.APPLICATION_XML)
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
        System.out.println("sessionToken:" + sessionToken);
        System.out.println("entityClass:" + entityClassName);
        System.out.println(xml);
        
        EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);
        MetamodelImpl metamodel = (MetamodelImpl)em.getEntityManagerFactory().getMetamodel();
        Class<?> entityClass = metamodel.getEntityClass(entityClassName);
        System.out.println("entityClass" + entityClass);
        
        Object entity = JAXBUtils.toObject(xml, entityClass);
        Object output = em.merge(entity);
        return output;
    }
    
    @DELETE
    @Consumes(MediaType.APPLICATION_XML)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/delete/{id}")
    public void delete(@PathParam("sessionToken") String sessionToken, 
            @PathParam("entityClass") String entityClassName, @PathParam("id") String id,
            InputStream in) {      
        
        
        System.out.println("sessionToken:" + sessionToken);
        System.out.println("entityClass:" + entityClassName);
        System.out.println("ID:" + id);
        
        String xml = null;
        try
        {
            xml = StreamUtils.toString(in);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        System.out.println(xml);
        
        EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);
        MetamodelImpl metamodel = (MetamodelImpl)em.getEntityManagerFactory().getMetamodel();
        Class<?> entityClass = metamodel.getEntityClass(entityClassName);
        System.out.println("entityClass" + entityClass);
        
        Object entity = JAXBUtils.toObject(xml, entityClass);
        em.remove(entity);        
    }

}
