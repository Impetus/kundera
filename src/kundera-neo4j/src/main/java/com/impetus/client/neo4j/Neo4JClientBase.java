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
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.client.ClientPropertiesSetter;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * Base class for all Neo4J clients
 * 
 * @author amresh.singh
 */
public abstract class Neo4JClientBase extends ClientBase implements ClientPropertiesSetter
{
    private static Logger log = LoggerFactory.getLogger(Neo4JClientBase.class);

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
            setBatchSize(Integer.valueOf(batch_Size));
        }
        else
        {
            PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(persistenceUnit);
            setBatchSize(puMetadata.getBatchSize());
        }
    }

    void setBatchSize(int batch_Size)
    {
        this.batchSize = batch_Size;
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
