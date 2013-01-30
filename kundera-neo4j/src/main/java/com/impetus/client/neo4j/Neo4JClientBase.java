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

import java.lang.reflect.Field;

import javax.persistence.metamodel.Attribute;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;

import com.impetus.client.neo4j.index.AutoIndexing;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.client.ClientPropertiesSetter;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * Base class for all Neo4J clients
 * @author amresh.singh
 */
public abstract class Neo4JClientBase extends ClientBase implements ClientPropertiesSetter
{
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
     * If node auto-indexing is disabled,manually index this Node
     * @param entityMetadata
     * @param graphDb
     * @param autoIndexing
     * @param node
     */
    protected void manuallyIndexNode(EntityMetadata entityMetadata, GraphDatabaseService graphDb,
            Node node)
    {
        AutoIndexing autoIndexing = new AutoIndexing();
        if (!autoIndexing.isNodeAutoIndexingEnabled(graphDb) && entityMetadata.isIndexable())
        {
            MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                    entityMetadata.getPersistenceUnit());
            IndexManager index = graphDb.index();
            Index<Node> nodeIndex = index.forNodes(entityMetadata.getIndexName());
            
            //ID attribute has to be indexed necessarily
            nodeIndex.add(node, entityMetadata.getIdAttribute().getName(), node.getProperty(entityMetadata.getIdAttribute().getName()));
            
            // Index all other fields, for whom indexing is enabled
            for (Attribute attribute : metaModel.entity(entityMetadata.getEntityClazz()).getSingularAttributes())
            {
                Field field = (Field) attribute.getJavaMember();
                if (!attribute.isCollection() && !attribute.isAssociation()
                        && entityMetadata.getIndexProperties().keySet().contains(field.getName()))
                {
                    nodeIndex.add(node, field.getName(), node.getProperty(field.getName()));
                }
            }

        }
    }  
    
    /**
     * If relationship auto-indexing is disabled, manually index this relationship
     * @param entityMetadata
     * @param graphDb
     * @param autoIndexing
     * @param node
     */
    protected void manuallyIndexRelationship(EntityMetadata entityMetadata, GraphDatabaseService graphDb,
            Relationship relationship)
    {
        AutoIndexing autoIndexing = new AutoIndexing();
        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                entityMetadata.getPersistenceUnit());
        
        if (!autoIndexing.isRelationshipAutoIndexingEnabled(graphDb) && entityMetadata.isIndexable())
        {
            
            IndexManager index = graphDb.index();
            Index<Relationship> relationshipIndex = index.forRelationships(entityMetadata.getIndexName());
            
            //ID attribute has to be indexed first
            relationshipIndex.add(relationship, entityMetadata.getIdAttribute().getName(), relationship.getProperty(entityMetadata.getIdAttribute().getName()));
            
            //Index all other fields, for whom indexing is enabled
            for (Attribute attribute : metaModel.entity(entityMetadata.getEntityClazz()).getSingularAttributes())
            {
                Field field = (Field) attribute.getJavaMember();
                if (!attribute.isCollection() && !attribute.isAssociation()
                        && entityMetadata.getIndexProperties().keySet().contains(field.getName()))
                {
                    relationshipIndex.add(relationship, field.getName(), relationship.getProperty(field.getName()));
                }
            }
            
        }
    }  
}
