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
package com.impetus.client.neo4j;

import java.util.Map;
import java.util.Properties;

import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.neo4j.config.Neo4JPropertyReader;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

/**
 * Factory of {@link Neo4JRESTClient}
 * 
 * @author amresh.singh
 */
public class Neo4JRESTClientFactory extends GenericClientFactory
{
    /** The logger. */
    private static Logger log = LoggerFactory.getLogger(Neo4JRESTClientFactory.class);

    @Override
    public void initialize(Map<String, Object> puProperties)
    {
        initializePropertyReader();
    }

    @Override
    protected Object createPoolOrConnection()
    {
        if(log.isInfoEnabled()) log.info("Getting Service root for Neo4J REST connection");
        PersistenceUnitMetadata puMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata()
                .getPersistenceUnitMetadata(getPersistenceUnit());

        Properties props = puMetadata.getProperties();

        String host = null;
        String port = null;

        if (externalProperties != null)
        {
            host = (String) externalProperties.get(PersistenceProperties.KUNDERA_NODES);
            port = (String) externalProperties.get(PersistenceProperties.KUNDERA_PORT);

        }
        if (host == null)
        {
            host = (String) props.get(PersistenceProperties.KUNDERA_NODES);
        }
        if (port == null)
        {
            port = (String) props.get(PersistenceProperties.KUNDERA_PORT);
        }

        final String SERVER_ROOT_URI = "http://" + host + ":" + port + "/db/data/"; 
        
        
        WebResource resource = (WebResource) getConnectionPoolOrConnection();
        
        if(resource == null)
        {
            resource = com.sun.jersey.api.client.Client.create().resource(SERVER_ROOT_URI);
        }       
        
        WebResource.Builder builder = resource.path(SERVER_ROOT_URI).accept(MediaType.APPLICATION_JSON);
        
        ClientResponse response = (ClientResponse) builder.get(ClientResponse.class);
        
        if(response.getStatus() == 200) {
            if(log.isInfoEnabled()) log.info(String.format("GET on [%s], status code [%d]", SERVER_ROOT_URI, response.getStatus()));            
        }
        else
        {
            log.error("Could not connect to " + SERVER_ROOT_URI + "; Connection to Neo4J failed.");
            return null;
        }
        response.close();

        return resource;
    }

    @Override
    protected Client instantiateClient(String persistenceUnit)
    {
        return new Neo4JRESTClient(this);
    }

    @Override
    public boolean isThreadSafe()
    {
        return true;
    }
    
    @Override
    public void destroy()
    {
    }

    @Override
    public SchemaManager getSchemaManager(Map<String, Object> puProperties)
    {
        log.error("No schema manager implementation available for Neo4J, returning null");
        return null;
    }

    /**
     * 
     */
    private void initializePropertyReader()
    {
        if (propertyReader == null)
        {
            propertyReader = new Neo4JPropertyReader();
            propertyReader.read(getPersistenceUnit());
        }
    }

}
