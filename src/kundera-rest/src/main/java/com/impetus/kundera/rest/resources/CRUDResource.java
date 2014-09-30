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

import javax.persistence.EntityManager;
import javax.persistence.metamodel.EmbeddableType;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.rest.common.Constants;
import com.impetus.kundera.rest.common.EntityUtils;
import com.impetus.kundera.rest.common.JAXBUtils;
import com.impetus.kundera.rest.common.ResponseBuilder;
import com.impetus.kundera.rest.repository.EMRepository;

/**
 * REST resource for CRUD operations
 * 
 * @author amresh.singh
 */

@Path("/" + Constants.KUNDERA_API_PATH + Constants.CRUD_RESOURCE_PATH + "/{entityClass}")
public class CRUDResource {
    /** log for this class. */
    private static Logger log = LoggerFactory.getLogger(CRUDResource.class);


    /**
     * Handler for POST method requests for this resource Inserts an entity into datastore
     * 
     * @param sessionToken
     * @param entityClassName
     * @param in
     * @return
     */
    @POST
    // @Consumes({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    public Response insert(@HeaderParam(Constants.SESSION_TOKEN_HEADER_NAME) String sessionToken,
        @PathParam("entityClass") String entityClassName, String input, @Context HttpHeaders headers) {
        String mediaType = headers != null && headers.getRequestHeaders().containsKey("Content-type")? headers.getRequestHeader("Content-type").get(0) : MediaType.APPLICATION_JSON;

        mediaType =
            mediaType.equalsIgnoreCase(MediaType.APPLICATION_XML) ? MediaType.APPLICATION_XML
                : MediaType.APPLICATION_JSON;
        sessionToken = sessionToken.replaceAll("^\"|\"$", "");
        input = input.replaceAll("^\"|\"$", "");

        if (log.isDebugEnabled()) {
            log.debug("POST: SessionToken: " + sessionToken);
            log.debug("POST: entityClass: " + entityClassName);
        }

        try {
            EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);
            Class<?> entityClass = EntityUtils.getEntityClass(entityClassName, em);

            if (log.isDebugEnabled())
                log.debug("POST: entityClass" + entityClass);

            if (log.isDebugEnabled())
                log.debug("POST: Media Type:" + mediaType);

            log.debug("Entity Data" + input);

            Object entity = JAXBUtils.toObject(input, entityClass, mediaType);

            log.debug("Entity Data" + entity);
            em.persist(entity);

        } catch (Exception e) {
            log.error(e.getMessage());
            return Response.serverError().build();
        }

        return Response.ok("Record persisted", mediaType).build();
    }

    /**
     * Handler for GET method requests for this resource Finds an entity from datastore
     * 
     * @param sessionToken
     * @param entityClassName
     * @param id
     * @return
     */
    @GET
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @Path("/{id}")
    public Response find(@HeaderParam(Constants.SESSION_TOKEN_HEADER_NAME) String sessionToken,
        @PathParam("entityClass") String entityClassName, @PathParam("id") String id, @Context HttpHeaders headers) {

        sessionToken = sessionToken.replaceAll("^\"|\"$", "");
        String mediaType = headers != null && headers.getRequestHeaders().containsKey("Content-type")? headers.getRequestHeader("Content-type").get(0) : MediaType.APPLICATION_JSON;
        mediaType =
            mediaType.equalsIgnoreCase(MediaType.APPLICATION_XML) ? MediaType.APPLICATION_XML
                : MediaType.APPLICATION_JSON;
        log.debug("GET: sessionToken:" + sessionToken);
        log.debug("GET: entityClass:" + entityClassName);
        log.debug("GET: ID:" + id);

        Object entity = null;
        Class<?> entityClass;
        EntityMetadata entityMetadata = null;
        try {
            EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);
            entityClass = EntityUtils.getEntityClass(entityClassName, em);
            entityMetadata = EntityUtils.getEntityMetaData(entityClass.getSimpleName(), em);
            log.debug("GET: entityClass" + entityClass);
            MetamodelImpl metaModel = (MetamodelImpl) em.getEntityManagerFactory().getMetamodel();
            EmbeddableType keyObj = null;
            Object key = null;
            id = java.net.URLDecoder.decode(id, "UTF-8");
            if (metaModel.isEmbeddable(entityMetadata.getIdAttribute().getBindableJavaType())) {
                keyObj = metaModel.embeddable(entityMetadata.getIdAttribute().getBindableJavaType());
                key = JAXBUtils.toObject(id, keyObj.getJavaType(), mediaType);
            } else {
                ObjectMapper mapper = new ObjectMapper();
                key = mapper.convertValue(id, entityMetadata.getIdAttribute().getBindableJavaType());
            }
            
            entity = em.find(entityClass, key);
        } catch (Exception e) {
            log.error(e.getMessage());
            return Response.serverError().build();
        }

        log.debug("GET: " + entity);

        if (entity == null) {
            return Response.noContent().build();
        }
        String output = JAXBUtils.toString(entityClass, entity, mediaType);
        if (mediaType.equalsIgnoreCase(MediaType.APPLICATION_JSON)) {
            return Response.ok(ResponseBuilder.buildOutput(entityClass, entityMetadata, output), mediaType).build();
        } else {
            return Response.ok(output.toString(), mediaType).build();
        }
    }

    /**
     * Handler for PUT method requests for this resource Updates an entity into datastore
     * 
     * @param sessionToken
     * @param entityClassName
     * @param in
     * @return
     */
    @PUT
    // @Consumes({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    public Response update(@HeaderParam(Constants.SESSION_TOKEN_HEADER_NAME) String sessionToken,
        @PathParam("entityClass") String entityClassName, String input, @Context HttpHeaders headers) {
        sessionToken = sessionToken.replaceAll("^\"|\"$", "");
        input = input.replaceAll("^\"|\"$", "");

        log.debug("PUT: sessionToken:" + sessionToken);
        log.debug("PUT: entityClassName:" + entityClassName);
        String mediaType = headers != null && headers.getRequestHeaders().containsKey("Content-type")? headers.getRequestHeader("Content-type").get(0) : MediaType.APPLICATION_JSON;
        mediaType =
            mediaType.equalsIgnoreCase(MediaType.APPLICATION_XML) ? MediaType.APPLICATION_XML
                : MediaType.APPLICATION_JSON;

        log.debug("POST: Media Type:" + mediaType);

        Object output;
        Class<?> entityClass;
        Object entity;
        EntityMetadata entityMetadata = null;
        try {
            EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);
            entityClass = EntityUtils.getEntityClass(entityClassName, em);
            log.debug("PUT: entityClass: " + entityClass);
            entityMetadata = EntityUtils.getEntityMetaData(entityClass.getSimpleName(), em);

            entity = JAXBUtils.toObject(input, entityClass, mediaType);
            output = em.merge(entity);
        } catch (Exception e) {
            log.error(e.getMessage());
            return Response.serverError().build();
        }

        if (output == null) {
            return Response.notModified().build();
        }
        output = JAXBUtils.toString(entityClass, output, mediaType);
        if (mediaType.equalsIgnoreCase(MediaType.APPLICATION_JSON)) {
            return Response.ok(ResponseBuilder.buildOutput(entityClass, entityMetadata, output), mediaType).build();
        } else {
            return Response.ok(output.toString(), mediaType).build();
        }
    }

    /**
     * Handler for DELETE method requests for this resource Deletes an entity from datastore
     * 
     * @param sessionToken
     * @param entityClassName
     * @param id
     * @return
     */
    @DELETE
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @Path("/delete/{id}")
    public Response delete(@HeaderParam(Constants.SESSION_TOKEN_HEADER_NAME) String sessionToken,
        @PathParam("entityClass") String entityClassName, @PathParam("id") String id, @Context HttpHeaders headers) {

        sessionToken = sessionToken.replaceAll("^\"|\"$", "");
        String mediaType = headers != null && headers.getRequestHeaders().containsKey("Content-type")? headers.getRequestHeader("Content-type").get(0) : MediaType.APPLICATION_JSON;
        mediaType =
            mediaType.equalsIgnoreCase(MediaType.APPLICATION_XML) ? MediaType.APPLICATION_XML
                : MediaType.APPLICATION_JSON;
        log.debug("DELETE: sessionToken:" + sessionToken);
        log.debug("DELETE: entityClass Name:" + entityClassName);
        log.debug("DELETE: ID:" + id);

        try {
            EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);
            Class<?> entityClass = EntityUtils.getEntityClass(entityClassName, em);
            log.debug("DELETE: entityClass" + entityClass);
            EntityMetadata entityMetadata = EntityUtils.getEntityMetaData(entityClass.getSimpleName(), em);
            MetamodelImpl metaModel = (MetamodelImpl) em.getEntityManagerFactory().getMetamodel();
            EmbeddableType keyObj = null;
            Object key = null;
            id = java.net.URLDecoder.decode(id, "UTF-8");
            if (metaModel.isEmbeddable(entityMetadata.getIdAttribute().getBindableJavaType())) {
                keyObj = metaModel.embeddable(entityMetadata.getIdAttribute().getBindableJavaType());
                key = JAXBUtils.toObject(id, keyObj.getJavaType(), mediaType);
            } else {
                ObjectMapper mapper = new ObjectMapper();
                key = mapper.convertValue(id, entityMetadata.getIdAttribute().getBindableJavaType());
            }

            Object entity = em.find(entityClass, key);
            em.remove(entity);
        } catch (Exception e) {
            log.error(e.getMessage());
            return Response.serverError().build();
        }

        return Response.ok(new String("Deleted Successfully"), mediaType).build();

    }

}