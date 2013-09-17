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
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.rest.common.Constants;
import com.impetus.kundera.rest.common.Response;
import com.impetus.kundera.rest.repository.EMRepository;

/**
 * REST resource for entity transactions
 * 
 * @author amresh.singh
 */

@Path("/" + Constants.KUNDERA_API_PATH + Constants.TRANSACTION_RESOURCE_PATH)
public class TransactionResource
{
    private static Logger log = LoggerFactory.getLogger(TransactionResource.class);

    /**
     * Handler for GET method requests for this resource Begins a fresh
     * transaction for given session
     * 
     * @return
     */
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String begin(@HeaderParam(Constants.SESSION_TOKEN_HEADER_NAME) String sessionToken)
    {
        if (log.isDebugEnabled())
            log.debug("GET: Session Token:" + sessionToken);

        EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);
        if (em == null)
        {
            if (log.isDebugEnabled())
                log.warn("GET: Session Token:" + sessionToken + " doesn't exist and transaction failed to begin");
            return Response.GET_TX_FAILED;
        }

        try
        {
            em.getTransaction().begin();
        }
        catch (Exception e)
        {
            log.error("GET: Failed: " + e.getMessage());
            return Response.GET_TX_FAILED;
        }
        return Response.GET_TX_SUCCESS;
    }

    /**
     * Handler for POST method requests for this resource Commits transaction
     * for the given session
     * 
     * @param applicationToken
     * @return
     */
    @POST
    @Consumes(MediaType.TEXT_PLAIN)
    public String commit(@HeaderParam(Constants.SESSION_TOKEN_HEADER_NAME) String sessionToken)
    {
        if (log.isDebugEnabled())
            log.debug("POST: Session Token:" + sessionToken);

        EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);
        if (em == null)
        {
            if (log.isDebugEnabled())
                log.warn("POST: Session Token:" + sessionToken + " doesn't exist and transaction could not be commited");
            return Response.POST_TX_FAILED;
        }

        try
        {
            em.getTransaction().commit();
        }
        catch (Exception e)
        {
            log.error("POST: Failed: " + e.getMessage());
            return Response.POST_TX_FAILED;
        }
        return Response.POST_TX_SUCCESS;
    }

    /**
     * Handler for DELETE method requests for this resource Rolls back
     * transaction for the given session
     * 
     * @return
     */
    @DELETE
    @Consumes(MediaType.TEXT_PLAIN)
    public String rollback(@HeaderParam(Constants.SESSION_TOKEN_HEADER_NAME) String sessionToken)
    {
        if (log.isDebugEnabled())
            log.debug("DELETE: Session Token:" + sessionToken);

        EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);
        if (em == null)
        {
            if (log.isDebugEnabled())
                log.warn("DELETE: Session Token:" + sessionToken
                        + " doesn't exist and transaction could not be rolled back");
            return Response.DELETE_TX_FAILED;

        }

        try
        {
            em.getTransaction().rollback();
        }
        catch (Exception e)
        {
            log.error("DELETE: Failed: " + e.getMessage());
            return Response.DELETE_TX_FAILED;
        }

        return Response.DELETE_TX_SUCCESS;
    }

}
