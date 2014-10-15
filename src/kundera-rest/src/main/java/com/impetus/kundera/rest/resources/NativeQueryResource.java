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
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.apache.cassandra.thrift.Column;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.rest.common.Constants;
import com.impetus.kundera.rest.common.EntityUtils;
import com.impetus.kundera.rest.common.ResponseBuilder;
import com.impetus.kundera.rest.converters.CollectionConverter;
import com.impetus.kundera.rest.repository.EMRepository;

/**
 * REST based resource for Native Queries
 * 
 * @author amresh.singh
 */

@Path("/" + Constants.KUNDERA_API_PATH + Constants.NATIVE_QUERY_RESOURCE_PATH)
public class NativeQueryResource {
    private static Logger log = LoggerFactory.getLogger(NativeQueryResource.class);

    /**
     * Executes Native Query and returns resposne
     * 
     * @param headers
     * @param info
     * @return
     */
    @POST
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @Path("/{entityClassName}")
    public Response executeNativeQuery(@Context HttpHeaders headers, @Context UriInfo info, String query) {

        String sessionToken = headers.getRequestHeader(Constants.SESSION_TOKEN_HEADER_NAME).get(0);

        String mediaType = headers != null && headers.getRequestHeaders().containsKey("Content-type")? headers.getRequestHeader("Content-type").get(0) : MediaType.APPLICATION_JSON;
        mediaType =
            mediaType.equalsIgnoreCase(MediaType.APPLICATION_XML) ? MediaType.APPLICATION_XML
                : MediaType.APPLICATION_JSON;
        String entityClassName = info.getPathParameters().getFirst("entityClassName");
        if (log.isDebugEnabled())
            log.debug("GET:: Session Token:" + sessionToken + ", Entity Class Name:" + entityClassName
                + ", Native Query:" + query + ", Media Type:" + mediaType);

        List result = null;
        Class<?> entityClass = null;
        EntityMetadata entityMetadata = null;
        Query q;

        try {
            query = URLDecoder.decode(query, "UTF-8");
            sessionToken = sessionToken.replaceAll("^\"|\"$", "");
            EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);

            entityClass = EntityUtils.getEntityClass(entityClassName, em);
            entityMetadata = EntityUtils.getEntityMetaData(entityClass.getSimpleName(), em);
            if (log.isDebugEnabled())
                log.debug("GET: entityClass" + entityClass);
            q = em.createNativeQuery(query, entityClass);
            result = q.getResultList();
            result = onNativeCassResults(result, entityMetadata, em);
        } catch (Exception e) {
            log.error(e.getMessage());
            return Response.serverError().build();
        }

        if (result == null) {
            return Response.noContent().build();
        }

        if (log.isDebugEnabled())
            log.debug("GET: Media Type:" + mediaType);
       
    
        String output = CollectionConverter.toString(result, entityClass, mediaType);
        if (mediaType.equalsIgnoreCase(MediaType.APPLICATION_JSON)) {
            return Response.ok(ResponseBuilder.buildOutput(entityClass, entityMetadata, output), mediaType).build();
        } else {
            return Response.ok(output.toString(), mediaType).build();
        }

    }

    /**
     * @param result
     * @param entityMetadata
     * @param em
     * @return
     */
    private List onNativeCassResults(List result, EntityMetadata entityMetadata, EntityManager em) {
        Map<String, Client<Query>> clients = (Map<String, Client<Query>>) em.getDelegate();
        Client client = clients.get(entityMetadata.getPersistenceUnit());
        if((client.getClass().getSimpleName().equals("ThriftClient") || client.getClass().getSimpleName().equals("PelopsClient")
                        || client.getClass().getSimpleName().equals("DSClient"))
                        && Column.class.equals(result.get(0).getClass())) {
            int count = 0;
            for(Object column : result) {
                Map<Object, Object> valueMap = new HashMap<Object, Object>();
                valueMap.put(PropertyAccessorHelper.getObject(String.class, ((Column) column).getName()), PropertyAccessorHelper.getObject(Long.class, ((Column) column).getValue()));
                result.set(count, valueMap);
                count ++;
            }
                      
           
        }
        return result;
    }

    /**
     * Executes Native script and returns responsne
     * 
     * @param headers
     * @param info
     * @return
     */
    @PUT
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @Path("/{persistenceUnit}")
    public Response executeNativeScript(@Context HttpHeaders headers, @Context UriInfo info, String script) {

        String sessionToken = headers.getRequestHeader(Constants.SESSION_TOKEN_HEADER_NAME).get(0);
        String persistenceUnit = info.getPathParameters().getFirst("persistenceUnit");
        String mediaType = headers != null && headers.getRequestHeaders().containsKey("Content-type")? headers.getRequestHeader("Content-type").get(0) : MediaType.APPLICATION_JSON;
        mediaType =
            mediaType.equalsIgnoreCase(MediaType.APPLICATION_XML) ? MediaType.APPLICATION_XML
                : MediaType.APPLICATION_JSON;

        if (log.isDebugEnabled())
            log.debug("GET:: Session Token:" + sessionToken + ", Native Script:" + script + ", Media Type:" + mediaType);

        Object result = null;

        try {
            script = URLDecoder.decode(script, "UTF-8");
            sessionToken = sessionToken.replaceAll("^\"|\"$", "");
            EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);

            Map<String, Client<Query>> clients = (Map<String, Client<Query>>) em.getDelegate();
            Client client = clients.get(persistenceUnit);

            result = client.executeScript(script);

        } catch (Exception e) {
            log.error(e.getMessage());
            return Response.serverError().build();
        }

        if (log.isDebugEnabled())
            log.debug("GET: Media Type:" + mediaType);

        if (result == null) {
            return Response.noContent().build();
        } else {
            if (mediaType.equalsIgnoreCase(MediaType.APPLICATION_JSON)) {
                ObjectMapper mapper = new ObjectMapper();
                String output = null;

                try {
                    output = mapper.writeValueAsString(result);
                } catch (JsonGenerationException e) {
                    log.error(e.getMessage());
                } catch (JsonMappingException e) {
                    log.error(e.getMessage());
                } catch (IOException e) {
                    log.error(e.getMessage());
                }
                return Response.ok(ResponseBuilder.buildOutput(output, "'"), mediaType).build();
            } else {
                return Response.ok(result, mediaType).build();
            }
        }

    }

    /************** Named Native Queries **************************/

    /**
     * Executes named native queries and returns response
     * 
     * @param headers
     * @param info
     * @return
     */

    @GET
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @Path("/{entityClass}/{namedNativeQueryName}")
    public Response executeNamedNativeQuery(@Context HttpHeaders headers, @Context UriInfo info) {

        String entityClassName = info.getPathParameters().getFirst("entityClass");
        String namedNativeQueryName = info.getPathParameters().getFirst("namedNativeQueryName");
        String sessionToken = headers.getRequestHeader(Constants.SESSION_TOKEN_HEADER_NAME).get(0);

        String mediaType = headers != null && headers.getRequestHeaders().containsKey("Content-type")? headers.getRequestHeader("Content-type").get(0) : MediaType.APPLICATION_JSON;
        mediaType =
            mediaType.equalsIgnoreCase(MediaType.APPLICATION_XML) ? MediaType.APPLICATION_XML
                : MediaType.APPLICATION_JSON;
        sessionToken = sessionToken.replaceAll("^\"|\"$", "");
        if (log.isDebugEnabled())
            log.debug("GET:: Session Token:" + sessionToken + ", Entity Class Name:" + entityClassName
                + ", Named Native Query:" + namedNativeQueryName + ", Media Type:" + mediaType);

        Class<?> entityClass = null;
        List result = null;
        EntityMetadata entityMetadata = null;
        try {
            EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);

            entityClass = EntityUtils.getEntityClass(entityClassName, em);
            entityMetadata = EntityUtils.getEntityMetaData(entityClass.getSimpleName(), em);
            if (log.isDebugEnabled())
                log.debug("GET: entityClass" + entityClass);
            Query q = em.createNamedQuery(namedNativeQueryName);
            result = q.getResultList();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return Response.serverError().build();
        }

        if (result == null) {
            return Response.noContent().build();
        }

        String output = CollectionConverter.toString(result, entityClass, mediaType);
        if (mediaType.equalsIgnoreCase(MediaType.APPLICATION_JSON)) {
            return Response.ok(ResponseBuilder.buildOutput(entityClass, entityMetadata, output), mediaType).build();
        } else {
            return Response.ok(output.toString(), mediaType).build();
        }
    }

}
