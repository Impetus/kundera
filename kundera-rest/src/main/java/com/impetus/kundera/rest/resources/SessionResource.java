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
import javax.persistence.EntityManagerFactory;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.rest.common.Constants;
import com.impetus.kundera.rest.common.Response;
import com.impetus.kundera.rest.common.TokenUtils;
import com.impetus.kundera.rest.repository.EMFRepository;
import com.impetus.kundera.rest.repository.EMRepository;

/**
 * Session Token REST Resource
 * 
 * @author amresh.singh
 */

@Path("/" + Constants.KUNDERA_API_PATH + Constants.SESSION_RESOURCE_PATH)
public class SessionResource
{
    private static Log log = LogFactory.getLog(SessionResource.class);

    /**
     * Handler for GET method requests for this resource Generates Session token
     * and returns, creates and puts EM into repository
     * 
     * @param applicationToken
     * @return
     */
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Consumes(MediaType.TEXT_PLAIN)
    public String getSessionToken(@HeaderParam(Constants.APPLICATION_TOKEN_HEADER_NAME) String applicationToken)
    {
        if (log.isDebugEnabled())
            log.debug("GET: Application Token:" + applicationToken);

        EntityManagerFactory emf = EMFRepository.INSTANCE.getEMF(applicationToken);

        if (emf == null)
        {
            if (log.isDebugEnabled())
                log.warn("GET: Application Token:" + applicationToken
                        + " doesn't exist and hence Session can't be created");
            return Response.GET_ST_FAILED;
        }

        String sessionToken = TokenUtils.generateSessionToken();
        EntityManager em = emf.createEntityManager();

        EMRepository.INSTANCE.addEm(sessionToken, em);
        return sessionToken;
    }

    /**
     * Handler for GET method requests for this resource Generates Session token
     * and returns, creates and puts EM into repository
     * 
     * @param applicationToken
     * @return
     */
    @PUT
    @Produces(MediaType.TEXT_PLAIN)
    @Consumes(MediaType.TEXT_PLAIN)
    public String flush(@HeaderParam(Constants.SESSION_TOKEN_HEADER_NAME) String sessionToken)
    {
        if (log.isDebugEnabled())
            log.debug("PUT: Session Token:" + sessionToken);

        EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);
        if (em == null)
        {
            if (log.isDebugEnabled())
                log.warn("PUT: Session Token:" + sessionToken + " doesn't exist and hence can't be deleted");
            return Response.PUT_ST_FAILED;
        }

        try
        {
            em.flush();
        }
        catch (Exception e)
        {
            log.error("PUT: Failed: " + e.getMessage());
            return Response.PUT_ST_FAILED;
        }
        return Response.PUT_ST_SUCCESS;
    }

    /**
     * Handler for DELETE method requests for this resource Closes EM and
     * removes session token alongwith from repository
     * 
     * @param id
     * @return
     */
    @DELETE
    @Produces(MediaType.TEXT_PLAIN)
    @Consumes(MediaType.TEXT_PLAIN)
    public String deleteSession(@HeaderParam(Constants.SESSION_TOKEN_HEADER_NAME) String sessionToken)
    {
        if (log.isDebugEnabled())
            log.debug("DELETE: Session Token:" + sessionToken);

        EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);
        if (em == null)
        {
            if (log.isDebugEnabled())
                log.warn("DELETE: Session Token:" + sessionToken + " doesn't exist and hence can't be deleted");
            return Response.DELETE_ST_FAILED;
        }

        EMRepository.INSTANCE.removeEm(sessionToken);
        return Response.DELETE_ST_SUCCESS;
    }

}
