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

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import javax.ws.rs.DELETE;
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.rest.common.Constants;
import com.impetus.kundera.rest.common.EntityUtils;
import com.impetus.kundera.rest.converters.CollectionConverter;
import com.impetus.kundera.rest.repository.EMRepository;

/**
 * REST based resource for Native Queries
 * 
 * @author amresh.singh
 */

@Path("/" + Constants.KUNDERA_API_PATH + Constants.NATIVE_QUERY_RESOURCE_PATH)
public class NativeQueryResource
{
    private static Logger log = LoggerFactory.getLogger(NativeQueryResource.class);

    /************** Native Queries **************************/

    /**
     * Handler for POST method requests for this resource Retrieves records from
     * datasource for a given Native query
     * 
     * @return
     */

    @POST
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @Path("/{entityClassName}/q={nativeQuery}")
    public Response executeInsertNativeQuery(@Context HttpHeaders headers, @Context UriInfo info)
    {
        return executeNativeQuery(headers, info);
    }

    /**
     * Handler for GET method requests for this resource Retrieves records from
     * datasource for a given Native query
     * 
     * @return
     */

    @GET
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @Path("/{entityClassName}/q={nativeQuery}")
    public Response executeSelectNativeQuery(@Context HttpHeaders headers, @Context UriInfo info)
    {
        return executeNativeQuery(headers, info);
    }

    /**
     * Handler for PUT method requests for this resource Retrieves records from
     * datasource for a given Native query
     * 
     * @return
     */

    @PUT
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @Path("/{entityClassName}/q={nativeQuery}")
    public Response executeUpdateNativeQuery(@Context HttpHeaders headers, @Context UriInfo info)
    {
        return executeNativeQuery(headers, info);
    }

    /**
     * Handler for DELETE method requests for this resource Retrieves records
     * from datasource for a given Native query
     * 
     * @return
     */

    @DELETE
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @Path("/{entityClassName}/q={nativeQuery}")
    public Response executeDeleteNativeQuery(@Context HttpHeaders headers, @Context UriInfo info)
    {
        return executeNativeQuery(headers, info);
    }

    /************** Named Native Queries **************************/

    /**
     * Handler for POST method requests for this resource Retrieves all entities
     * for the given named native query
     * 
     * @return
     */

    @POST
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @Path("/{entityClass}/{namedNativeQueryName}")
    public Response executeInsertNamedNativeQuery(@Context HttpHeaders headers, @Context UriInfo info)
    {
        return executeNamedNativeQuery(headers, info);
    }

    /**
     * Handler for GET method requests for this resource Retrieves all entities
     * for the given named native query
     */
    @GET
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @Path("/{entityClass}/{namedNativeQueryName}")
    public Response executeSelectNamedNativeQuery(@Context HttpHeaders headers, @Context UriInfo info)
    {
        return executeNamedNativeQuery(headers, info);
    }

    /**
     * Handler for PUT method requests for this resource Retrieves all entities
     * for the given named native query
     */
    @PUT
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @Path("/{entityClass}/{namedNativeQueryName}")
    public Response executeUpdateNamedNativeQuery(@Context HttpHeaders headers, @Context UriInfo info)
    {
        return executeNamedNativeQuery(headers, info);
    }

    /**
     * Handler for DELETE method requests for this resource Retrieves all
     * entities for the given named native query
     */
    @DELETE
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @Path("/{entityClass}/{namedNativeQueryName}")
    public Response executeDeleteNamedNativeQuery(@Context HttpHeaders headers, @Context UriInfo info)
    {
        return executeNamedNativeQuery(headers, info);
    }

    /**
     * Executes Native Query and returns resposne
     * 
     * @param headers
     * @param info
     * @return
     */
    private Response executeNativeQuery(HttpHeaders headers, UriInfo info)
    {
        String entityClassName = info.getPathParameters().getFirst("entityClassName");
        String nativeQueryName = info.getPathParameters().getFirst("nativeQuery");
        String sessionToken = headers.getRequestHeader(Constants.SESSION_TOKEN_HEADER_NAME).get(0);
        String mediaType = headers.getRequestHeader("accept").get(0);

        if (log.isDebugEnabled())
            log.debug("GET:: Session Token:" + sessionToken + ", Entity Class Name:" + entityClassName
                    + ", Native Query:" + nativeQueryName + ", Media Type:" + mediaType);

        List result = null;
        Class<?> entityClass = null;
        Query q;
        try
        {
            EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);

            entityClass = EntityUtils.getEntityClass(entityClassName, em);
            if (log.isDebugEnabled())
                log.debug("GET: entityClass" + entityClass);
            if (entityClass == null)
            {
                return Response.serverError().build();
            }

            q = em.createNativeQuery(nativeQueryName, entityClass);
            result = q.getResultList();
        }
        catch (Exception e)
        {
            log.error(e.getMessage());
            return Response.serverError().build();
        }

        if (result == null)
        {
            return Response.noContent().build();
        }

        if (log.isDebugEnabled())
            log.debug("GET: Media Type:" + mediaType);

        String output = CollectionConverter.toString(result, entityClass, mediaType);
        return Response.ok(output).build();
    }

    /**
     * Executes named native queries and returns response
     * 
     * @param headers
     * @param info
     * @return
     */
    private Response executeNamedNativeQuery(HttpHeaders headers, UriInfo info)
    {
        String entityClassName = info.getPathParameters().getFirst("entityClass");
        String namedNativeQueryName = info.getPathParameters().getFirst("namedNativeQueryName");
        String sessionToken = headers.getRequestHeader(Constants.SESSION_TOKEN_HEADER_NAME).get(0);
        String mediaType = headers.getRequestHeader("accept").get(0);

        if (log.isDebugEnabled())
            log.debug("GET:: Session Token:" + sessionToken + ", Entity Class Name:" + entityClassName
                    + ", Named Native Query:" + namedNativeQueryName + ", Media Type:" + mediaType);

        Class<?> entityClass = null;
        List result = null;

        try
        {
            EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);

            entityClass = EntityUtils.getEntityClass(entityClassName, em);
            if (log.isDebugEnabled())
                log.debug("GET: entityClass" + entityClass);
            if (entityClass == null)
            {
                return Response.serverError().build();
            }

            Query q = em.createNamedQuery(namedNativeQueryName);
            result = q.getResultList();
        }
        catch (Exception e)
        {
            log.error(e.getMessage());
            return Response.serverError().build();
        }

        if (result == null)
        {
            return Response.noContent().build();
        }

        String output = CollectionConverter.toString(result, entityClass, mediaType);
        return Response.ok(output).build();
    }

}
