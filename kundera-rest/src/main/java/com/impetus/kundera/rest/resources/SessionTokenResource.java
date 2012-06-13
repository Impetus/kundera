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
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
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
 * Session Token Resource
 * @author amresh.singh
 */

@Path(Constants.KUNDERA_API_PATH + Constants.SESSION_TOKEN_RESOURCE_PATH)
public class SessionTokenResource
{
    private static Log log = LogFactory.getLog(SessionTokenResource.class);
    
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Consumes(MediaType.TEXT_PLAIN)    
    @Path("/at/{applicationToken}")
    public String getSessionToken(@PathParam("applicationToken") String applicationToken) {        
        log.debug("GET: Application Token:" + applicationToken);
        
        EntityManagerFactory emf = EMFRepository.INSTANCE.getEMF(applicationToken);
        
        if(emf == null) {
            //Handle error
        }
        
        String sessionToken = TokenUtils.generateSessionToken();        
        EntityManager em = emf.createEntityManager();
        
        
        EMRepository.INSTANCE.addEm(sessionToken, em);        
        return sessionToken;
    } 
    
    @DELETE
    @Produces(MediaType.TEXT_PLAIN)
    @Consumes(MediaType.TEXT_PLAIN) 
    @Path("{id}")
    public String deleteSession(@PathParam("id") String id) {        
        log.debug("DELETE: Session Token:" + id);
        
        EntityManager em = EMRepository.INSTANCE.getEM(id);
        if(em == null) {
            //Handle Error
        }
        
        EMRepository.INSTANCE.removeEm(id);        
        return Response.DELETE_ST_SUCCESS;        
    }    
    
    

}
