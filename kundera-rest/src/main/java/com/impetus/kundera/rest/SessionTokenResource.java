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
package com.impetus.kundera.rest;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 * <Prove description of functionality provided by this Type> 
 * @author amresh.singh
 */

@Path("/kundera/api/session/at/{applicationToken}")
public class SessionTokenResource
{
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Consumes(MediaType.TEXT_PLAIN)    
    public String getSessionToken(@PathParam("applicationToken") String applicationToken) {        
        
        EntityManagerFactory emf = EMFRepository.INSTANCE.getEMF(applicationToken);
        
        if(emf == null) {
            //Handle error
        }
        
        String sessionToken = TokenUtils.generateSessionToken();        
        EntityManager em = emf.createEntityManager();
        
        
        EMRepository.INSTANCE.addEm(sessionToken, em);
        
        return sessionToken;
    }    
    
    

}
