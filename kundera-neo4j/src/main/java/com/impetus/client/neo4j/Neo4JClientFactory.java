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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.neo4j.config.Neo4JPropertyReader;
import com.impetus.client.neo4j.config.Neo4JPropertyReader.Neo4JSchemaMetadata;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.configure.ClientProperties;
import com.impetus.kundera.configure.ClientProperties.DataStore;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * Factory of Neo4J client(s) 
 * @author amresh.singh
 */
public class Neo4JClientFactory extends GenericClientFactory
{    

    /** The logger. */
    private static Logger log = LoggerFactory.getLogger(Neo4JClientFactory.class);
    

    @Override
    public void initialize(Map<String, Object> puProperties)
    {
        initializePropertyReader();
    }

    @Override
    protected Object createPoolOrConnection()
    {
        log.info("Initializing Neo4J database connection");

        PersistenceUnitMetadata puMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata()
                .getPersistenceUnitMetadata(getPersistenceUnit());

        Properties props = puMetadata.getProperties();
        String datastoreFilePath = (String) props.get(PersistenceProperties.KUNDERA_DATASTORE_FILE_PATH);

        Neo4JSchemaMetadata nsmd = Neo4JPropertyReader.nsmd;
        ClientProperties cp = nsmd != null ? nsmd.getClientProperties() : null;
        
        GraphDatabaseService graphDb = (GraphDatabaseService) getConnectionPoolOrConnection();
        
        if (cp != null)
        {
            DataStore dataStore = nsmd != null ? nsmd.getDataStore() : null;      
         
            Properties properties = dataStore != null && dataStore.getConnection() != null ? dataStore.getConnection()
                    .getProperties() : null;
                    
            if(properties != null)
            {
                Map<String, String> config = new HashMap<String, String>((Map)properties);
                graphDb = new ImpermanentGraphDatabase(config);
            }        
        }        
        
        if(graphDb == null)
        {
            graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(datastoreFilePath);
            registerShutdownHook(graphDb);           
        }       
        
        return graphDb;
    }

    @Override
    protected Client instantiateClient(String persistenceUnit)
    {
        return new Neo4JClient(this);
    }

    @Override
    public boolean isThreadSafe()
    {
        return false;
    }
    
    @Override
    public SchemaManager getSchemaManager(Map<String, Object> puProperties)
    {
        return null;
    }

    @Override
    public void destroy()
    {
    }
    
    GraphDatabaseService getConnection()
    {
        return (GraphDatabaseService)getConnectionPoolOrConnection();
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
    
    /**
     * Registers a shutdown hook for the Neo4j instance so that it
     * shuts down nicely when the VM exits (even if you "Ctrl-C" the
     * running example before it's completed)
     * @param graphDb
     */
    private static void registerShutdownHook(final GraphDatabaseService graphDb)
    {
        Runtime.getRuntime().addShutdownHook(new Thread()
        {
            @Override
            public void run()
            {
                graphDb.shutdown();
            }
        });
    }

}
