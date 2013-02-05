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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.persistence.PersistenceException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import com.impetus.client.neo4j.config.Neo4JPropertyReader;
import com.impetus.client.neo4j.config.Neo4JPropertyReader.Neo4JSchemaMetadata;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.client.ClientPropertiesSetter;
import com.impetus.kundera.configure.ClientProperties;
import com.impetus.kundera.configure.PersistenceUnitConfigurationException;
import com.impetus.kundera.configure.ClientProperties.DataStore;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * Base class for all Neo4J clients
 * @author amresh.singh
 */
public abstract class Neo4JClientBase extends ClientBase implements ClientPropertiesSetter
{
    private static Log log = LogFactory.getLog(Neo4JClientBase.class);
    
    /** Batch size. */
    protected int batchSize;
    
    /** list of nodes for batch processing. */
    protected List<Node> nodes = new ArrayList<Node>();
    
    protected boolean isEntityForNeo4J(EntityMetadata entityMetadata)
    {
        String persistenceUnit = entityMetadata.getPersistenceUnit();
        PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(persistenceUnit);
        String clientFactory = puMetadata.getProperty(PersistenceProperties.KUNDERA_CLIENT_FACTORY);
        if (clientFactory.indexOf("com.impetus.client.neo4j") >= 0)
        {
            return true;
        }
        return false;
    }  
    
    
    /**
     * @param persistenceUnit
     * @param puProperties
     */
    protected void populateBatchSize(String persistenceUnit, Map<String, Object> puProperties)
    {
        String batch_Size = puProperties != null ? (String) puProperties.get(PersistenceProperties.KUNDERA_BATCH_SIZE)
                : null;
        if (batch_Size != null)
        {
            batchSize = Integer.valueOf(batch_Size);
            if (batchSize == 0)
            {
                throw new IllegalArgumentException("kundera.batch.size property must be numeric and > 0");
            }
        }
        else
        {
            PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(persistenceUnit);
            batchSize = puMetadata.getBatchSize();
        }
    }

    public int getBatchSize()
    {
        return batchSize;
    }

    public void clear()
    {
        if (nodes != null)
        {
            nodes.clear();
            nodes = null;
            nodes = new ArrayList<Node>();
        }
    } 
    
    
}
