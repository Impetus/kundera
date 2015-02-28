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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import javax.persistence.PersistenceException;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.SingularAttribute;

import org.apache.commons.lang.StringUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.ReadableIndex;
import org.neo4j.graphdb.index.UniqueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.neo4j.index.Neo4JIndexManager;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.utils.KunderaCoreUtils;
import com.impetus.kundera.utils.ReflectUtils;

/**
 * Responsible for converting Neo4J graph (nodes+relationships) into JPA
 * entities and vice versa
 * 
 * @author amresh.singh
 */
public final class GraphEntityMapper
{
    /** Separator between constituents of composite key */
    private static final String COMPOSITE_KEY_SEPARATOR = "$CKS$";

    private static final String PROXY_NODE_TYPE_KEY = "$NODE_TYPE$";

    private static final String PROXY_NODE_VALUE = "$PROXY_NODE$";

    /** The log. */
    private static Logger log = LoggerFactory.getLogger(GraphEntityMapper.class);

    private Neo4JIndexManager indexer;

    private KunderaMetadata kunderaMetadata;

    public GraphEntityMapper(Neo4JIndexManager indexer, final KunderaMetadata kunderaMetadata)
    {
        this.indexer = indexer;
        this.kunderaMetadata = kunderaMetadata;
    }

    /**
     * Fetches(and converts) {@link Node} instance from Entity object If it's a
     * update operation, node is searched and attributes populated Otherwise
     * Node is created into database (replacing any existing node) with
     * attributes populated
     * 
     * @param id
     * 
     */
    public Node getNodeFromEntity(Object entity, Object key, GraphDatabaseService graphDb, EntityMetadata m,
            boolean isUpdate)
    {

        // Construct top level node first, making sure unique ID in the index
        Node node = null;
        if (!isUpdate)
        {
            node = getOrCreateNodeWithUniqueFactory(entity, key, m, graphDb);
        }
        else
        {

            node = searchNode(key, m, graphDb, true);
        }

        if (node != null)
        {
            populateNodeProperties(entity, m, node);
        }

        return node;
    }

    /**
     * Create "Proxy" nodes into Neo4J. Proxy nodes are defined as nodes in
     * Neo4J that refer to a record in some other database. They cater to
     * polyglot persistence cases.
     */
    public Node createProxyNode(Object sourceNodeId, Object targetNodeId, GraphDatabaseService graphDb,
            EntityMetadata sourceEntityMetadata, EntityMetadata targetEntityMetadata)
    {

        String sourceNodeIdColumnName = ((AbstractAttribute) sourceEntityMetadata.getIdAttribute()).getJPAColumnName();
        String targetNodeIdColumnName = ((AbstractAttribute) targetEntityMetadata.getIdAttribute()).getJPAColumnName();

        Node node = graphDb.createNode();
        node.setProperty(PROXY_NODE_TYPE_KEY, PROXY_NODE_VALUE);
        node.setProperty(sourceNodeIdColumnName, sourceNodeId);
        node.setProperty(targetNodeIdColumnName, targetNodeId);
        return node;
    }

    /**
     * 
     * Converts a {@link Node} instance to entity object
     */
    public Object getEntityFromNode(Node node, EntityMetadata m)
    {
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());
        EntityType entityType = metaModel.entity(m.getEntityClazz());

        // Iterate over, entity attributes
        Set<Attribute> attributes = entityType.getSingularAttributes();

        Object entity = null;

        try
        {
            // entity = m.getEntityClazz().newInstance();

            for (Attribute attribute : attributes)
            {
                Field field = (Field) attribute.getJavaMember();
                String columnName = ((AbstractAttribute) attribute).getJPAColumnName();

                // Set Entity level properties
                if (metaModel.isEmbeddable(m.getIdAttribute().getBindableJavaType())
                        && m.getIdAttribute().getJavaType().equals(field.getType()))
                {
                    Object idValue = deserializeIdAttributeValue(m, (String) node.getProperty(columnName));
                    if (idValue != null)
                    {
                        entity = initialize(m, entity);
                        PropertyAccessorHelper.set(entity, field, idValue);
                    }
                }
                else if (!attribute.isCollection() && !attribute.isAssociation()
                        && !((AbstractAttribute) m.getIdAttribute()).getJPAColumnName().equals(columnName))
                {
                    Object columnValue = node.getProperty(columnName, null);
                    if (columnValue != null)
                    {
                        entity = initialize(m, entity);
                        PropertyAccessorHelper.set(entity, field, fromNeo4JObject(columnValue, field));
                    }
                }
            }

            if (entity != null && !metaModel.isEmbeddable(m.getIdAttribute().getBindableJavaType()))
            {
                Object rowKey = node.getProperty(((AbstractAttribute) m.getIdAttribute()).getJPAColumnName());
                if (rowKey != null)
                {
                    PropertyAccessorHelper.setId(entity, m,
                            fromNeo4JObject(rowKey, (Field) m.getIdAttribute().getJavaMember()));
                }
            }
        }
        catch (NotFoundException e)
        {
            log.info(e.getMessage());
            return null;
        }

        return entity;
    }

    /**
     * 
     * Converts a {@link Relationship} object to corresponding entity object
     */
    public Object getEntityFromRelationship(Relationship relationship, EntityMetadata topLevelEntityMetadata,
            Relation relation)
    {
        EntityMetadata relationshipEntityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata,
                relation.getMapKeyJoinClass());

        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                relationshipEntityMetadata.getPersistenceUnit());
        EntityType entityType = metaModel.entity(relationshipEntityMetadata.getEntityClazz());

        // Iterate over, entity attributes
        Set<Attribute> attributes = entityType.getSingularAttributes();

        Object entity = null;

        try
        {
            // entity =
            // relationshipEntityMetadata.getEntityClazz().newInstance();

            for (Attribute attribute : attributes)
            {
                Field field = (Field) attribute.getJavaMember();
                String columnName = ((AbstractAttribute) attribute).getJPAColumnName();

                // Set Entity level properties
                if (metaModel.isEmbeddable(relationshipEntityMetadata.getIdAttribute().getBindableJavaType())
                        && relationshipEntityMetadata.getIdAttribute().getJavaType().equals(field.getType()))
                {
                    Object idValue = deserializeIdAttributeValue(relationshipEntityMetadata,
                            (String) relationship.getProperty(columnName));
                    if (idValue != null)
                    {
                        entity = initialize(relationshipEntityMetadata, entity);
                        PropertyAccessorHelper.set(entity, field, idValue);
                    }
                }
                else if (!attribute.isCollection() && !attribute.isAssociation()
                        && !field.getType().equals(topLevelEntityMetadata.getEntityClazz())
                        && !field.getType().equals(relation.getTargetEntity()))
                {
                    Object value = relationship.getProperty(columnName, null);
                    if (value != null)
                    {
                        entity = initialize(relationshipEntityMetadata, entity);
                        PropertyAccessorHelper.set(entity, field, fromNeo4JObject(value, field));
                    }
                }
            }

            if (entity != null
                    && !metaModel.isEmbeddable(relationshipEntityMetadata.getIdAttribute().getBindableJavaType()))
            {
                Object rowKey = relationship.getProperty(((AbstractAttribute) relationshipEntityMetadata
                        .getIdAttribute()).getJPAColumnName());
                if (rowKey != null)
                {
                    PropertyAccessorHelper
                            .setId(entity,
                                    relationshipEntityMetadata,
                                    fromNeo4JObject(rowKey, (Field) relationshipEntityMetadata.getIdAttribute()
                                            .getJavaMember()));
                }
            }
        }
        catch (NotFoundException e)
        {
            log.info(e.getMessage());
            return null;
        }

        return entity;
    }

    /**
     * Creates a Map containing all properties (and their values) for a given
     * entity
     * 
     * @param entity
     * @param m
     * @return
     */
    public Map<String, Object> createNodeProperties(Object entity, EntityMetadata m)
    {
        Map<String, Object> props = new HashMap<String, Object>();

        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());

        EntityType entityType = metaModel.entity(m.getEntityClazz());

        // Iterate over entity attributes
        Set<Attribute> attributes = entityType.getSingularAttributes();
        for (Attribute attribute : attributes)
        {
            Field field = (Field) attribute.getJavaMember();

            // Set Node level properties
            if (!attribute.isCollection() && !attribute.isAssociation())
            {

                String columnName = ((AbstractAttribute) attribute).getJPAColumnName();
                Object value = PropertyAccessorHelper.getObject(entity, field);
                if (value != null)
                {
                    props.put(columnName, toNeo4JProperty(value));
                }
            }
        }
        return props;
    }

    /**
     * Populates Node properties from Entity object
     * 
     * @param entity
     * @param m
     * @param node
     */
    private void populateNodeProperties(Object entity, EntityMetadata m, Node node)
    {
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());
        EntityType entityType = metaModel.entity(m.getEntityClazz());

        // Iterate over entity attributes
        Set<Attribute> attributes = entityType.getSingularAttributes();
        for (Attribute attribute : attributes)
        {
            Field field = (Field) attribute.getJavaMember();

            // Set Node level properties
            if (!attribute.isCollection() && !attribute.isAssociation() && !((SingularAttribute) attribute).isId())
            {
                String columnName = ((AbstractAttribute) attribute).getJPAColumnName();
                Object value = PropertyAccessorHelper.getObject(entity, field);
                if (value != null)
                {
                    node.setProperty(columnName, toNeo4JProperty(value));
                }
            }
        }
    }

    /**
     * Populates a {@link Relationship} with attributes of a given relationship
     * entity object <code>relationshipObj</code>
     * 
     * @param entityMetadata
     * @param targetNodeMetadata
     * @param relationship
     * @param relationshipObj
     */
    public void populateRelationshipProperties(EntityMetadata entityMetadata, EntityMetadata targetNodeMetadata,
            Relationship relationship, Object relationshipObj)
    {
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                entityMetadata.getPersistenceUnit());
        EntityType entityType = metaModel.entity(relationshipObj.getClass());

        Set<Attribute> attributes = entityType.getSingularAttributes();
        for (Attribute attribute : attributes)
        {
            Field f = (Field) attribute.getJavaMember();
            if (!f.getType().equals(entityMetadata.getEntityClazz())
                    && !f.getType().equals(targetNodeMetadata.getEntityClazz()))
            {
                EntityMetadata relMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata,
                        relationshipObj.getClass());
                String columnName = ((AbstractAttribute) attribute).getJPAColumnName();
                if (metaModel.isEmbeddable(relMetadata.getIdAttribute().getBindableJavaType())
                        && ((SingularAttribute) attribute).isId())
                {
                    // Populate Embedded attribute value into relationship
                    Object id = PropertyAccessorHelper.getId(relationshipObj, relMetadata);
                    Object serializedIdValue = serializeIdAttributeValue(relMetadata, metaModel, id);
                    relationship.setProperty(columnName, serializedIdValue);

                    // Populate attributes of embedded fields into relationship
                    Set<Attribute> embeddableAttributes = metaModel.embeddable(
                            relMetadata.getIdAttribute().getBindableJavaType()).getSingularAttributes();
                    for (Attribute embeddableAttribute : embeddableAttributes)
                    {
                        String embeddedColumn = ((AbstractAttribute) embeddableAttribute).getJPAColumnName();
                        if (embeddedColumn == null)
                            embeddedColumn = embeddableAttribute.getName();
                        Object value = PropertyAccessorHelper
                                .getObject(id, (Field) embeddableAttribute.getJavaMember());

                        if (value != null)
                            relationship.setProperty(embeddedColumn, value);
                    }
                }
                else
                {
                    Object value = PropertyAccessorHelper.getObject(relationshipObj, f);
                    relationship.setProperty(columnName, toNeo4JProperty(value));
                }

            }
        }

    }

    /**
     * Creates a Map containing all properties (and their values) for a given
     * relationship entity
     * 
     * @param entityMetadata
     * @param targetEntityMetadata
     * @param relationshipProperties
     * @param relationshipEntity
     */
    public Map<String, Object> createRelationshipProperties(EntityMetadata entityMetadata,
            EntityMetadata targetEntityMetadata, Object relationshipEntity)
    {
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                entityMetadata.getPersistenceUnit());
        EntityType entityType = metaModel.entity(relationshipEntity.getClass());
        Map<String, Object> relationshipProperties = new HashMap<String, Object>();

        Set<Attribute> attributes = entityType.getSingularAttributes();
        for (Attribute attribute : attributes)
        {
            Field f = (Field) attribute.getJavaMember();
            if (!f.getType().equals(entityMetadata.getEntityClazz())
                    && !f.getType().equals(targetEntityMetadata.getEntityClazz()))
            {
                String relPropertyName = ((AbstractAttribute) attribute).getJPAColumnName();
                Object value = PropertyAccessorHelper.getObject(relationshipEntity, f);
                relationshipProperties.put(relPropertyName, toNeo4JProperty(value));
            }
        }
        return relationshipProperties;
    }

    /**
     * 
     * Gets (if available) or creates a node for the given entity
     */
    private Node getOrCreateNodeWithUniqueFactory(final Object entity, final Object id, final EntityMetadata m,
            GraphDatabaseService graphDb)
    {
        final String idColumnName = ((AbstractAttribute) m.getIdAttribute()).getJPAColumnName();
        final MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());
        final String idUniqueValue = serializeIdAttributeValue(m, metaModel, id);

        UniqueFactory<Node> factory = new UniqueFactory.UniqueNodeFactory(graphDb, m.getIndexName())
        {
            /**
             * Initialize ID attribute
             */
            @Override
            protected void initialize(Node created, Map<String, Object> properties)
            {
                // Set Embeddable ID attribute
                if (metaModel.isEmbeddable(m.getIdAttribute().getBindableJavaType()))
                {
                    // Populate embedded field value in serialized format
                    created.setProperty(idColumnName, idUniqueValue);

                    // Populated all other attributes of embedded into this node
                    Set<Attribute> embeddableAttributes = metaModel
                            .embeddable(m.getIdAttribute().getBindableJavaType()).getSingularAttributes();
                    for (Attribute attribute : embeddableAttributes)
                    {
                        String columnName = ((AbstractAttribute) attribute).getJPAColumnName();
                        if (columnName == null)
                            columnName = attribute.getName();
                        Object value = PropertyAccessorHelper.getObject(id, (Field) attribute.getJavaMember());

                        if (value != null)
                            created.setProperty(columnName, value);
                    }
                }
                else
                {
                    created.setProperty(idColumnName, properties.get(idColumnName));
                }

            }
        };

        if (metaModel.isEmbeddable(m.getIdAttribute().getBindableJavaType()))
        {
            return factory.getOrCreate(idColumnName, idUniqueValue);
        }
        else
        {
            return factory.getOrCreate(idColumnName, id);
        }

    }

    /**
     * Prepares ID column value for embedded IDs by combining its attributes
     * 
     * @param m
     * @param id
     * @param metaModel
     * @return
     */
    private String serializeIdAttributeValue(final EntityMetadata m, MetamodelImpl metaModel, Object id)
    {
        if (!metaModel.isEmbeddable(m.getIdAttribute().getBindableJavaType()))
        {
            return null;
        }

        Class<?> embeddableClass = m.getIdAttribute().getBindableJavaType();
        String idUniqueValue = "";

        for (Field embeddedField : embeddableClass.getDeclaredFields())
        {
            if (!ReflectUtils.isTransientOrStatic(embeddedField))
            {

                Object value = PropertyAccessorHelper.getObject(id, embeddedField);
                if (value != null && !StringUtils.isEmpty(value.toString()))
                    idUniqueValue = idUniqueValue + value + COMPOSITE_KEY_SEPARATOR;
            }
        }

        if (idUniqueValue.endsWith(COMPOSITE_KEY_SEPARATOR))
            idUniqueValue = idUniqueValue.substring(0, idUniqueValue.length() - COMPOSITE_KEY_SEPARATOR.length());
        return idUniqueValue;
    }

    /**
     * Prepares Embedded ID field from value prepared via
     * serializeIdAttributeValue method.
     */
    private Object deserializeIdAttributeValue(final EntityMetadata m, String idValue)
    {
        if (idValue == null)
        {
            return null;
        }
        Class<?> embeddableClass = m.getIdAttribute().getBindableJavaType();
        Object embeddedObject = embeddedObject = KunderaCoreUtils.createNewInstance(embeddableClass);
        List<String> tokens = new ArrayList<String>();
        StringTokenizer st = new StringTokenizer((String) idValue, COMPOSITE_KEY_SEPARATOR);
        while (st.hasMoreTokens())
        {
            tokens.add((String) st.nextElement());
        }

        int count = 0;
        for (Field embeddedField : embeddableClass.getDeclaredFields())
        {
            if (!ReflectUtils.isTransientOrStatic(embeddedField))
            {
                if (count < tokens.size())
                {
                    String value = tokens.get(count++);
                    PropertyAccessorHelper.set(embeddedObject, embeddedField, value);
                }
            }
        }
        return embeddedObject;
    }

    /**
     * Gets (if available) or creates a relationship for the given entity
     */
    private Relationship getOrCreateRelationshipWithUniqueFactory(Object entity, EntityMetadata m,
            GraphDatabaseService graphDb)
    {
        Object id = PropertyAccessorHelper.getObject(entity, (Field) m.getIdAttribute().getJavaMember());
        final String idFieldName = m.getIdAttribute().getName();

        UniqueFactory<Relationship> factory = new UniqueFactory.UniqueRelationshipFactory(graphDb, m.getIndexName())
        {

            @Override
            protected Relationship create(Map<String, Object> paramMap)
            {
                return null;
            }

            @Override
            protected void initialize(Relationship relationship, Map<String, Object> properties)
            {
                relationship.setProperty(idFieldName, properties.get(idFieldName));
            }
        };

        return factory.getOrCreate(idFieldName, id);
    }

    /**
     * Converts a given field value to an object that is Neo4J compatible
     * 
     * @param source
     * @return
     */
    public Object toNeo4JProperty(Object source)
    {
        if (source instanceof BigDecimal || source instanceof BigInteger)
        {
            return source.toString();
        }
        else if ((source instanceof Calendar) || (source instanceof GregorianCalendar))
        {
            return PropertyAccessorHelper.fromSourceToTargetClass(String.class, Date.class,
                    ((Calendar) source).getTime());
        }
        if (source instanceof Date)
        {
            Class<?> sourceClass = source.getClass();
            return PropertyAccessorHelper.fromSourceToTargetClass(String.class, sourceClass, source);
        }
        return source;
    }

    /**
     * Converts a property stored in Neo4J (nodes or relationship) to
     * corresponding entity field value
     */
    public Object fromNeo4JObject(Object source, Field field)
    {
        Class<?> targetClass = field.getType();

        if (targetClass.isAssignableFrom(BigDecimal.class) || targetClass.isAssignableFrom(BigInteger.class))
        {
            return PropertyAccessorHelper.fromSourceToTargetClass(field.getType(), source.getClass(), source);
        }
        else if (targetClass.isAssignableFrom(Calendar.class) || targetClass.isAssignableFrom(GregorianCalendar.class))
        {
            Date d = (Date) PropertyAccessorHelper.fromSourceToTargetClass(Date.class, source.getClass(), source);
            Calendar cal = Calendar.getInstance();
            cal.setTime(d);
            return cal;
        }
        else if (targetClass.isAssignableFrom(Date.class))
        {
            return PropertyAccessorHelper.fromSourceToTargetClass(field.getType(), source.getClass(), source);
        }
        else
        {
            return source;
        }

    }

    /**
     * Searches a node from the database for a given key
     */
    public Node searchNode(Object key, EntityMetadata m, GraphDatabaseService graphDb, boolean skipProxy)
    {
        Node node = null;
        String idColumnName = ((AbstractAttribute) m.getIdAttribute()).getJPAColumnName();

        final MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());
        if (metaModel.isEmbeddable(m.getIdAttribute().getBindableJavaType()))
        {
            key = serializeIdAttributeValue(m, metaModel, key);
        }

        if (indexer.isNodeAutoIndexingEnabled(graphDb))
        {
            // Get the Node auto index
            ReadableIndex<Node> autoNodeIndex = graphDb.index().getNodeAutoIndexer().getAutoIndex();
            IndexHits<Node> nodesFound = autoNodeIndex.get(idColumnName, key);
            node = getMatchingNodeFromIndexHits(nodesFound, skipProxy);
        }
        else
        {
            // Searching within manually created indexes
            Index<Node> nodeIndex = graphDb.index().forNodes(m.getIndexName());
            IndexHits<Node> nodesFound = nodeIndex.get(idColumnName, key);
            node = getMatchingNodeFromIndexHits(nodesFound, skipProxy);
        }

        return node;
    }

    /**
     * Fetches first Non-proxy node from Index Hits
     * 
     * @param skipProxy
     * @param nodesFound
     * @return
     */
    protected Node getMatchingNodeFromIndexHits(IndexHits<Node> nodesFound, boolean skipProxy)
    {
        Node node = null;
        try
        {
            if (nodesFound == null || nodesFound.size() == 0 || !nodesFound.hasNext())
            {
                return null;
            }
            else
            {
                if (skipProxy)
                    node = getNonProxyNode(nodesFound);
                else
                    node = nodesFound.next();
            }
        }
        finally
        {
            nodesFound.close();
        }
        return node;
    }

    /**
     * Fetches Non-proxy nodes from index hits
     */
    private Node getNonProxyNode(IndexHits<Node> nodesFound)
    {
        Node node = null;
        if (nodesFound.hasNext())
        {
            node = nodesFound.next();
        }
        else
        {
            return null;
        }

        try
        {
            Object proxyNodeProperty = node.getProperty(PROXY_NODE_TYPE_KEY);
        }
        catch (NotFoundException e)
        {
            return node;
        }
        catch (IllegalStateException e)
        {
            return node;
        }
        return getNonProxyNode(nodesFound);
    }

    /**
     * 
     * @param m
     * @param entity
     * @param id
     * @return
     */
    private Object initialize(EntityMetadata m, Object entity)
    {
        try
        {
            if (entity == null)
            {
                entity = KunderaCoreUtils.createNewInstance(m.getEntityClazz());
            }

            return entity;
        }
        catch (Exception e)
        {
            throw new PersistenceException("Error occured while instantiating entity, Caused by : ", e);
        }
    }
}
