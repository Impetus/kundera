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

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.rest.common.Constants;
import com.impetus.kundera.rest.common.TokenUtils;
import com.impetus.kundera.rest.repository.EMFRepository;

/**
 * Application Token Resource
 * @author amresh.singh
 */

@Path(Constants.KUNDERA_API_PATH + Constants.APPLICATION_TOKEN_RESOURCE_PATH + "/pu/{persistenceUnits}")
public class ApplicationTokenResource
{  
    private static Log log = LogFactory.getLog(ApplicationTokenResource.class);
    
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Consumes(MediaType.TEXT_PLAIN)    
    public String getApplicationToken(@PathParam("persistenceUnits") String persistenceUnits) {
        log.debug("GET Persistence Unit(s):" + persistenceUnits);
        
        EntityManagerFactory emf = Persistence.createEntityManagerFactory(persistenceUnits);
        
        String applicationToken = TokenUtils.generateApplicationToken();        
        
        EMFRepository.INSTANCE.addEmf(applicationToken, emf);
        
        return applicationToken;
    }    

}
