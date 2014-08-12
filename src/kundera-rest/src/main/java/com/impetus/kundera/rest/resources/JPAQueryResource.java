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

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.query.QueryImpl;
import com.impetus.kundera.rest.common.Constants;
import com.impetus.kundera.rest.common.EntityUtils;
import com.impetus.kundera.rest.common.ResponseBuilder;
import com.impetus.kundera.rest.converters.CollectionConverter;
import com.impetus.kundera.rest.repository.EMRepository;

/**
 * REST based resource for JPA queries
 * 
 * @author amresh
 * 
 */

@Path("/" + Constants.KUNDERA_API_PATH + Constants.JPA_QUERY_RESOURCE_PATH)
public class JPAQueryResource {

    private static Logger log = LoggerFactory.getLogger(JPAQueryResource.class);

    /**
     * Handler for GET method requests for this resource. Retrieves all entities for a given table from datasource that
     * match after running named query. If named query=All, it returns all records.
     * 
     * @param sessionToken
     * @param entityClassName
     * @param id
     * @return
     */

    @GET
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @Path("/{entityClass}/{namedQueryName}")
    public Response executeNamedQuery(@Context HttpHeaders headers, @Context UriInfo info) {

        String entityClassName = info.getPathParameters().getFirst("entityClass");
        String namedQueryName = info.getPathParameters().getFirst("namedQueryName");
        String params = info.getRequestUri().getQuery();
        String sessionToken = headers.getRequestHeader(Constants.SESSION_TOKEN_HEADER_NAME).get(0);
        String mediaType = headers != null && headers.getRequestHeaders().containsKey("Content-type")? headers.getRequestHeader("Content-type").get(0) : MediaType.APPLICATION_JSON;
        mediaType =
            mediaType.equalsIgnoreCase(MediaType.APPLICATION_XML) ? MediaType.APPLICATION_XML
                : MediaType.APPLICATION_JSON;

        if (log.isDebugEnabled())
            log.debug("GET: sessionToken:" + sessionToken + ", entityClass:" + entityClassName + ", Named Query:"
                + namedQueryName + ", Media Type:" + mediaType);

        List result = null;
        Class<?> entityClass = null;
        EntityMetadata entityMetadata = null;
        try {
            sessionToken = sessionToken.replaceAll("^\"|\"$", "");
            EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);
            entityClass = EntityUtils.getEntityClass(entityClassName, em);
            entityMetadata = EntityUtils.getEntityMetaData(entityClass.getSimpleName(), em);

            if (log.isDebugEnabled())
                log.debug("GET: entityClass" + entityClass);

            if (Constants.NAMED_QUERY_ALL.equalsIgnoreCase(namedQueryName)) {
                String alias = entityClassName.substring(0, 1).toLowerCase();

                StringBuilder sb =
                    new StringBuilder().append("SELECT ").append(alias).append(" FROM ").append(entityClassName)
                        .append(" ").append(alias);

                Query q = em.createQuery(sb.toString());
                result = q.getResultList();
            } else {
                String queryPart = EntityUtils.getQueryPart(namedQueryName);
                String paramPart = params != null ? params : EntityUtils.getParameterPart(namedQueryName);

                Query q = em.createNamedQuery(queryPart);
                if (q == null) {
                    return Response.serverError().build();
                }

                boolean isDeleteOrUpdateQuery = ((QueryImpl) q).getKunderaQuery().isDeleteUpdate();
                if (isDeleteOrUpdateQuery) {
                    log.error("Incorrect HTTP method GET for query:" + queryPart);
                    return Response.noContent().build();
                }

                EntityUtils.setQueryParameters(queryPart, paramPart, q);

                result = q.getResultList();
            }

        } catch (Exception e) {
            log.error(e.getMessage());
            return Response.serverError().build();
        }

        if (log.isDebugEnabled())
            log.debug("GET: Result of " + namedQueryName + " Query : " + result);

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

    /**
     * Handler for GET method requests for this resource Retrieves records from datasource for a given select JPA query
     * 
     * @param sessionToken
     * @param entityClassName
     * @param id
     * @return
     */

    @POST
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @Path("/{jpaQuery}")
    public Response executeSelectJPAQuery(@Context HttpHeaders headers, @Context UriInfo info, String parameters) {
        String jpaQuery = info.getPathParameters().getFirst("jpaQuery");
        String params = info.getRequestUri().getQuery();

        String sessionToken = headers.getRequestHeader(Constants.SESSION_TOKEN_HEADER_NAME).get(0);
        sessionToken = sessionToken.replaceAll("^\"|\"$", "");
        String mediaType = headers != null && headers.getRequestHeaders().containsKey("Content-type")? headers.getRequestHeader("Content-type").get(0) : MediaType.APPLICATION_JSON;
        mediaType =
            mediaType.equalsIgnoreCase(MediaType.APPLICATION_XML) ? MediaType.APPLICATION_XML
                : MediaType.APPLICATION_JSON;
        if (log.isDebugEnabled())
            log.debug("GET: sessionToken:" + sessionToken + ", jpaQuery:" + jpaQuery + ", Media Type:" + mediaType);

        if (!EntityUtils.isValidQuery(jpaQuery, HttpMethod.GET)) {
            log.error("Incorrect HTTP method GET for query:" + jpaQuery);
            return Response.noContent().build();
        }

        List result = null;
        Query q = null;
        EntityMetadata entityMetadata = null;
        EntityManager em = null;
        try {
            em = EMRepository.INSTANCE.getEM(sessionToken);

            String queryPart = EntityUtils.getQueryPart(jpaQuery);
            String paramPart = params != null ? params : EntityUtils.getParameterPart(jpaQuery);

            q = em.createQuery(queryPart);
            if (q == null) {
                return Response.serverError().build();
            }

            EntityUtils.setQueryParameters(queryPart, paramPart, q);
            EntityUtils.setObjectQueryParameters(queryPart, parameters, q, em, mediaType);

            result = q.getResultList();
        } catch (Exception e) {
            log.error(e.getMessage());
            return Response.serverError().build();
        }

        if (log.isDebugEnabled())
            log.debug("GET: Result for JPA Query: " + result);

        if (result == null) {
            return Response.noContent().build();
        }

        Class<?> genericClass = ((QueryImpl) q).getKunderaQuery().getEntityClass();
        entityMetadata = EntityUtils.getEntityMetaData(genericClass.getSimpleName(), em);
        String output = CollectionConverter.toString(result, genericClass, mediaType);
        if (mediaType.equalsIgnoreCase(MediaType.APPLICATION_JSON)) {
            return Response.ok(ResponseBuilder.buildOutput(genericClass, entityMetadata, output), mediaType).build();
          
        } else {
            return Response.ok(output.toString(), mediaType).build();
        }

    }

    /**
     * Handler for PUT method requests for this resource Retrieves records from datasource for a given UPDATE JPA query
     * 
     * @param sessionToken
     * @param entityClassName
     * @param id
     * @return
     */
    @PUT
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @Path("/{jpaQuery}")
    public Response executeUpdateJPAQuery(@Context HttpHeaders headers, @Context UriInfo info, String parameters) {
        String jpaQuery = info.getPathParameters().getFirst("jpaQuery");
        String params = info.getRequestUri().getQuery();
        String sessionToken = headers.getRequestHeader(Constants.SESSION_TOKEN_HEADER_NAME).get(0);
        sessionToken = sessionToken.replaceAll("^\"|\"$", "");
        String mediaType = headers != null && headers.getRequestHeaders().containsKey("Content-type")? headers.getRequestHeader("Content-type").get(0) : MediaType.APPLICATION_JSON;
        mediaType =
            mediaType.equalsIgnoreCase(MediaType.APPLICATION_XML) ? MediaType.APPLICATION_XML
                : MediaType.APPLICATION_JSON;
        if (log.isDebugEnabled())
            log.debug("GET: sessionToken:" + sessionToken + ", jpaQuery:" + jpaQuery + ", Media Type:" + mediaType);

        if (!EntityUtils.isValidQuery(jpaQuery, HttpMethod.PUT)) {
            log.error("Incorrect HTTP method POST for query:" + jpaQuery);
            return Response.noContent().build();
        }

        int result = executeWrite(jpaQuery, params, sessionToken, parameters, mediaType);
        if (log.isDebugEnabled())
            log.debug("GET: Result for JPA Query: " + result);

        if (result < 0) {
            return Response.noContent().build();
        }

        return Response.ok(result, mediaType).build();
    }

    /**
     * Handler for DELETE method requests for this resource Retrieves records from datasource for a given UPDATE JPA
     * query
     * 
     * @param sessionToken
     * @param entityClassName
     * @param id
     * @return
     */
    @DELETE
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @Path("/{jpaQuery}")
    public Response executeDeleteJPAQuery(@Context HttpHeaders headers, @Context UriInfo info, String parameters) {
        String jpaQuery = info.getPathParameters().getFirst("jpaQuery");
        String params = info.getRequestUri().getQuery();
        String sessionToken = headers.getRequestHeader(Constants.SESSION_TOKEN_HEADER_NAME).get(0);
        sessionToken = sessionToken.replaceAll("^\"|\"$", "");
        String mediaType = headers != null && headers.getRequestHeaders().containsKey("Content-type")? headers.getRequestHeader("Content-type").get(0) : MediaType.APPLICATION_JSON;
        mediaType =
            mediaType.equalsIgnoreCase(MediaType.APPLICATION_XML) ? MediaType.APPLICATION_XML
                : MediaType.APPLICATION_JSON;
        if (log.isDebugEnabled())
            log.debug("GET: sessionToken:" + sessionToken + ", jpaQuery:" + jpaQuery + ", Media Type:" + mediaType);

        if (!EntityUtils.isValidQuery(jpaQuery, HttpMethod.DELETE)) {
            log.error("Incorrect HTTP method POST for query:" + jpaQuery);
            return Response.noContent().build();
        }

        int result = executeWrite(jpaQuery, params, sessionToken, parameters, mediaType);
        if (log.isDebugEnabled())
            log.debug("GET: Result for JPA Query: " + result);

        if (result < 0) {
            return Response.noContent().build();
        }

        return Response.ok(result, mediaType).build();
    }

    private int executeWrite(String jpaQuery, String params, String sessionToken, String parameters, String mediaType) {
        int result = -1;
        Query q = null;
        try {
            EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);

            String queryPart = EntityUtils.getQueryPart(jpaQuery);
            String paramPart = params != null ? params : EntityUtils.getParameterPart(jpaQuery);

            q = em.createQuery(queryPart);

            if (q != null) {
                EntityUtils.setQueryParameters(queryPart, paramPart, q);
                EntityUtils.setObjectQueryParameters(queryPart, parameters, q, em, mediaType);
                result = q.executeUpdate();
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return result;
    }

}
