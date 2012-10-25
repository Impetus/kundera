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
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.query.QueryImpl;
import com.impetus.kundera.rest.common.Constants;
import com.impetus.kundera.rest.common.EntityUtils;
import com.impetus.kundera.rest.converters.CollectionConverter;
import com.impetus.kundera.rest.repository.EMRepository;

/**
 * REST based resource for JPA queries
 * 
 * @author amresh
 * 
 */

@Path(Constants.KUNDERA_API_PATH + Constants.JPA_QUERY_RESOURCE_PATH)
public class JPAQueryResource
{

    private static Log log = LogFactory.getLog(JPAQueryResource.class);

    /**
     * Handler for GET method requests for this resource Retrieves all entities
     * for a given table from datasource
     * 
     * @param sessionToken
     * @param entityClassName
     * @param id
     * @return
     */

    @GET
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @Path("/{entityClass}/{namedQueryName}")
    public Response executeNamedQuery(@HeaderParam(Constants.SESSION_TOKEN_HEADER_NAME) String sessionToken,            
            @PathParam("entityClass") String entityClassName,
            @PathParam("namedQueryName") String namedQueryName,
            @Context HttpHeaders headers)
    {

        log.debug("GET: sessionToken:" + sessionToken + ", entityClass:" + entityClassName + ", Named Query:" + namedQueryName);
        

        List result = null;
        Class<?> entityClass = null;
        try
        {
            EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);
            entityClass = EntityUtils.getEntityClass(entityClassName, em);
            log.debug("GET: entityClass" + entityClass);

            if(Constants.NAMED_QUERY_ALL.equalsIgnoreCase(namedQueryName))
            {
                String alias = entityClassName.substring(0, 1).toLowerCase();

                StringBuilder sb = new StringBuilder().append("SELECT ").append(alias).append(" FROM ")
                        .append(entityClassName).append(" ").append(alias);

                Query q = em.createQuery(sb.toString());
                result = q.getResultList(); 
            }
            else
            {             
                Query q = em.createNamedQuery(namedQueryName);
                if(q == null)
                {
                    return Response.serverError().build();
                }   
                
                for(String headerName : headers.getRequestHeaders().keySet())
                {
                    if(headerName != null && headerName.startsWith(Constants.QUERY_PARAMS_HEADER_NAME_PREFIX))
                    {
                        String paramName = headerName.substring(Constants.QUERY_PARAMS_HEADER_NAME_PREFIX.length());
                        Object val = headers.getRequestHeaders().getFirst(headerName);
                        
                        if(StringUtils.isNumeric(paramName))
                        {
                            q.setParameter(Integer.parseInt(paramName), val);
                        }
                        else
                        {
                            q.setParameter(paramName, val);
                        }
                        
                    }
                }                
                
                result = q.getResultList();
            }            
            
        }
        catch (Exception e)
        {
            log.error(e.getMessage());
            e.printStackTrace();
            return Response.serverError().build();
        }

        log.debug("GET: Result of " + namedQueryName + " Query : " + result);

        if (result == null)
        {
            return Response.noContent().build();
        }

        String mediaType = headers.getRequestHeader("accept").get(0);
        log.debug("GET: Media Type:" + mediaType);

        String output = CollectionConverter.toString(result, entityClass, mediaType);

        return Response.ok(output).build();

    }

    /**
     * Handler for GET method requests for this resource Retrieves records from
     * datasource for a given JPA query
     * 
     * @param sessionToken
     * @param entityClassName
     * @param id
     * @return
     */

    @GET
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @Path("/{jpaQuery}")
    public Response executeQuery(@HeaderParam(Constants.SESSION_TOKEN_HEADER_NAME) String sessionToken,
            @PathParam("jpaQuery") String jpaQuery, @Context HttpHeaders headers)
    {

        log.debug("GET: sessionToken:" + sessionToken + ", jpaQuery:" + jpaQuery);

        List result = null;
        Query q = null;
        try
        {
            EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);

            q = em.createQuery(jpaQuery);

            result = q.getResultList();
        }
        catch (Exception e)
        {
            log.error(e.getMessage());
            return Response.serverError().build();
        }

        log.debug("GET: Result for JPA Query: " + result);

        if (result == null)
        {
            return Response.noContent().build();
        }

        String mediaType = headers.getRequestHeader("accept").get(0);
        log.debug("GET: Media Type:" + mediaType);

        Class<?> genericClass = ((QueryImpl) q).getKunderaQuery().getEntityClass();

        String output = CollectionConverter.toString(result, genericClass, mediaType);

        return Response.ok(output).build();
    }

}
