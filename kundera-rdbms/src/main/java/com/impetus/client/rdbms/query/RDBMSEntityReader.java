/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/
package com.impetus.client.rdbms.query;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.PersistenceException;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.Metamodel;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.rdbms.HibernateClient;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.metadata.model.Relation.ForeignKey;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.AbstractEntityReader;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.EntityReaderException;
import com.impetus.kundera.query.KunderaQuery.FilterClause;
import com.impetus.kundera.query.QueryHandlerException;

/**
 * The Class RDBMSEntityReader.
 * 
 * @author vivek.mishra
 */
public class RDBMSEntityReader extends AbstractEntityReader implements EntityReader
{

    /** the log used by this class. */
    private static Logger log = LoggerFactory.getLogger(RDBMSEntityReader.class);

    /** The conditions. */
    private Queue conditions;

    /** The filter. */
    private String filter;

    /** The jpa query. */
    private String jpaQuery;

    /**
     * Instantiates a new rDBMS entity reader.
     * 
     * @param luceneQuery
     *            the lucene query
     * @param query
     *            the query
     */
    public RDBMSEntityReader(String luceneQuery, String query)
    {
        this.luceneQueryFromJPAQuery = luceneQuery;
        this.jpaQuery = query;
    }

    /**
     * Instantiates a new rDBMS entity reader.
     */
    public RDBMSEntityReader()
    {

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.persistence.EntityReader#populateRelation(com.impetus
     * .kundera.metadata.model.EntityMetadata, java.util.List, boolean,
     * com.impetus.kundera.client.Client)
     */
    @Override
    public List<EnhanceEntity> populateRelation(EntityMetadata m, Client client, int maxResults)
    {
        // TODO: maxresults to be taken care after work on pagination.
        
        List<EnhanceEntity> ls = null;
        List<String> relationNames = m.getRelationNames();
        boolean isParent = m.isParent();
        if (!isParent)
        {
            // if it is not a parent.
            String sqlQuery = null;
            if (MetadataUtils.useSecondryIndex(client.getPersistenceUnit()))
            {
                sqlQuery = getSqlQueryFromJPA(m, relationNames, null);
            }
            else
            {
                // prepare lucene query and find.
                Set<String> rSet = fetchDataFromLucene(client);
                if (rSet != null && !rSet.isEmpty())
                {
                    filter = "WHERE";
                }
                sqlQuery = getSqlQueryFromJPA(m, relationNames, rSet);
            }
            // call client with relation name list and convert to sql query.

            ls = populateEnhanceEntities(m, relationNames, client, sqlQuery);

        }
        else
        {
            if (MetadataUtils.useSecondryIndex(client.getPersistenceUnit()))
            {
                try
                {
                    List entities = ((HibernateClient) client).find(getSqlQueryFromJPA(m, relationNames, null),
                            new ArrayList<String>(), m);
                    ls = new ArrayList<EnhanceEntity>(entities.size());
                    ls = transform(m, ls, entities);
                }
                catch (Exception e)
                {
                    log.error("Error while executing handleAssociation for cassandra:" + e.getMessage());
                    throw new QueryHandlerException(e);
                }
            }
            else
            {
                ls = onAssociationUsingLucene(m, client, ls);
            }

        }

        return ls;
    }

    /**
     * Populate enhance entities.
     * 
     * @param m
     *            the m
     * @param relationNames
     *            the relation names
     * @param client
     *            the client
     * @param sqlQuery
     *            the sql query
     * @return the list
     */
    private List<EnhanceEntity> populateEnhanceEntities(EntityMetadata m, List<String> relationNames, Client client,
            String sqlQuery)
    {
        List<EnhanceEntity> ls = null;
        List result = ((HibernateClient) client).find(sqlQuery, relationNames, m);

        try
        {
            if (!result.isEmpty())
            {
                ls = new ArrayList<EnhanceEntity>(result.size());
                for (Object o : result)
                {
                    Class clazz = m.getEntityClazz();
                    Object entity = clazz.newInstance();
                    boolean noRelationFound = true;
                    if (!o.getClass().isAssignableFrom(clazz))
                    {
                        entity = ((Object[]) o)[0];
                        noRelationFound = false;
                    }
                    else
                    {
                        entity = o;
                    }
                    EnhanceEntity e = new EnhanceEntity(entity, getId(entity, m), noRelationFound ? null
                            : populateRelations(relationNames, (Object[]) o));
                    ls.add(e);
                }
            }
        }
        catch (InstantiationException e)
        {
            log.error("Error during populating entities:" + e.getMessage());
            throw new EntityReaderException(e);
        }
        catch (IllegalAccessException e)
        {
            log.error("Error during populating entities:" + e.getMessage());
            throw new EntityReaderException(e);
        }
        return ls;

    }

    /**
     * Gets the sql query from jpa.
     * 
     * @param entityMetadata
     *            the entity metadata
     * @param relations
     *            the relations
     * @param primaryKeys
     *            the primary keys
     * @return the sql query from jpa
     */
    public String getSqlQueryFromJPA(EntityMetadata entityMetadata, List<String> relations, Set<String> primaryKeys)
    {
        ApplicationMetadata appMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata();
        Metamodel metaModel = appMetadata.getMetamodel(entityMetadata.getPersistenceUnit());
        if (appMetadata.isNative(jpaQuery))
        {
            return appMetadata.getQuery(jpaQuery);
        }

        String aliasName = "_" + entityMetadata.getTableName();

        StringBuilder queryBuilder = new StringBuilder("Select ");

        queryBuilder.append(aliasName);
        queryBuilder.append(".");
        queryBuilder.append(((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName());

        EntityType entityType = metaModel.entity(entityMetadata.getEntityClazz());
        Set<Attribute> attributes = entityType.getAttributes();
        for (Attribute field : attributes)
        //
        // for (String column : entityMetadata.getColumnFieldNames())
        {
            if (!field.isAssociation()
                    && !field.isCollection()
                    && !((Field) field.getJavaMember()).isAnnotationPresent(ManyToMany.class)
                    && !((AbstractAttribute) field).getJPAColumnName().equals(
                            ((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName())
                    && !((MetamodelImpl) metaModel).isEmbeddable(((AbstractAttribute) field).getBindableJavaType()))
            {
                queryBuilder.append(", ");
                queryBuilder.append(aliasName);
                queryBuilder.append(".");
                queryBuilder.append(((AbstractAttribute) field).getJPAColumnName());
            }
        }

        // Handle embedded columns, add them to list.
        // List<EmbeddedColumn> embeddedColumns =
        // entityMetadata.getEmbeddedColumnsAsList();
        Map<String, EmbeddableType> embeddedColumns = ((MetamodelImpl) metaModel).getEmbeddables(entityMetadata
                .getEntityClazz());
        for (EmbeddableType embeddedCol : embeddedColumns.values())
        {
            Set<Attribute> embeddedAttributes = embeddedCol.getAttributes();
            // for (Column column : embeddedCol.getColumns())
            for (Attribute column : embeddedAttributes)
            {
                queryBuilder.append(", ");
                queryBuilder.append(aliasName);
                queryBuilder.append(".");
                queryBuilder.append(((AbstractAttribute) column).getJPAColumnName());
            }
        }

        if (relations != null)
        {
            for (String relation : relations)
            {

                Relation rel = entityMetadata.getRelation(entityMetadata.getFieldName(relation));
                String r = MetadataUtils.getMappedName(entityMetadata, rel);
                if (!((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName().equalsIgnoreCase(
                        r != null ? r : relation)
                        && rel != null
                        && !rel.getProperty().isAnnotationPresent(ManyToMany.class)
                        && !rel.getProperty().isAnnotationPresent(OneToMany.class)
                        && (rel.getProperty().isAnnotationPresent(OneToOne.class)
                                && StringUtils.isBlank(rel.getMappedBy()) || rel.getProperty().isAnnotationPresent(
                                ManyToOne.class)))
                {
                    queryBuilder.append(", ");
                    queryBuilder.append(aliasName);
                    queryBuilder.append(".");
                    queryBuilder.append(r != null ? r : relation);
                }
            }
        }
        for (Relation r : entityMetadata.getRelations())
        {
            if (!r.getType().equals(ForeignKey.MANY_TO_MANY)
                    && (r.getProperty().isAnnotationPresent(OneToOne.class) && StringUtils.isBlank(r.getMappedBy()) || r
                            .getProperty().isAnnotationPresent(ManyToOne.class)))
            {
                queryBuilder.append(", ");
                queryBuilder.append(aliasName);
                queryBuilder.append(".");
                queryBuilder.append(r.getJoinColumnName());
            }
        }

        queryBuilder.append(" From ");
        if (entityMetadata.getSchema() != null && !entityMetadata.getSchema().isEmpty())
        {
            queryBuilder.append(entityMetadata.getSchema() + ".");
        }
        queryBuilder.append(entityMetadata.getTableName());
        queryBuilder.append(" ");
        queryBuilder.append(aliasName);
        // add conditions
        if (filter != null)
        {
            queryBuilder.append(" Where ");
            // queryBuilder.append(filter);
        }

        if (primaryKeys == null)
        {
            for (Object o : conditions)
            {

                if (o instanceof FilterClause)
                {
                    FilterClause clause = ((FilterClause) o);
                    // String fieldName = getColumnName(clause.getProperty());
                    String fieldName = clause.getProperty();
                    boolean isString = isStringProperty(entityType, fieldName, entityMetadata);

                    // queryBuilder.append(StringUtils.replace(clause.getProperty(),
                    // clause.getProperty().substring(0,
                    // clause.getProperty().indexOf(".")), aliasName));
                    queryBuilder.append(StringUtils.replace(clause.getProperty(), aliasName, aliasName));
                    queryBuilder.append(" ");
                    queryBuilder.append(clause.getCondition());

                    if (clause.getCondition().equalsIgnoreCase("like"))
                    {
                        queryBuilder.append("%");
                    }
                    queryBuilder.append(" ");
                    appendStringPrefix(queryBuilder, isString);
                    queryBuilder.append(clause.getValue());
                    appendStringPrefix(queryBuilder, isString);
                }
                else
                {
                    queryBuilder.append(" ");
                    queryBuilder.append(o);
                    queryBuilder.append(" ");
                }

            }
        }
        else
        {

            queryBuilder.append(aliasName);
            queryBuilder.append(".");
            queryBuilder.append(((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName());
            queryBuilder.append(" ");
            queryBuilder.append("IN(");
            int count = 0;
            Attribute col = entityMetadata.getIdAttribute();
            boolean isString = col.getJavaType().isAssignableFrom(String.class);
            for (String key : primaryKeys)
            {
                appendStringPrefix(queryBuilder, isString);
                queryBuilder.append(key);
                appendStringPrefix(queryBuilder, isString);
                if (++count != primaryKeys.size())
                {
                    queryBuilder.append(",");
                }
                else
                {
                    queryBuilder.append(")");
                }
            }

        }
        return queryBuilder.toString();
    }

    /**
     * Append string prefix.
     * 
     * @param queryBuilder
     *            the query builder
     * @param isString
     *            the is string
     */
    private void appendStringPrefix(StringBuilder queryBuilder, boolean isString)
    {
        if (isString)
        {
            queryBuilder.append("'");
        }
    }

    /**
     * Sets the conditions.
     * 
     * @param q
     *            the new conditions
     */
    public void setConditions(Queue q)
    {
        this.conditions = q;
    }

    /**
     * Sets the filter.
     * 
     * @param filter
     *            the new filter
     */
    public void setFilter(String filter)
    {
        this.filter = filter;
    }

    /**
     * Populate relations.
     * 
     * @param relations
     *            the relations
     * @param o
     *            the o
     * @return the map
     */
    private Map<String, Object> populateRelations(List<String> relations, Object[] o)
    {
        Map<String, Object> relationVal = new HashMap<String, Object>(relations.size());
        int counter = 1;
        for (String r : relations)
        {
            relationVal.put(r, o[counter++]);
        }
        return relationVal;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.persistence.EntityReader#findById(java.lang.Object,
     * com.impetus.kundera.metadata.model.EntityMetadata, java.util.List,
     * com.impetus.kundera.client.Client)
     */
    @Override
    public EnhanceEntity findById(Object primaryKey, EntityMetadata m, Client client)
    {
        List<String> relationNames = m.getRelationNames();
        if (relationNames != null && !relationNames.isEmpty())
        {
            Set<String> keys = new HashSet<String>(1);
            keys.add(primaryKey.toString());
            String query = getSqlQueryFromJPA(m, relationNames, keys);
            List<EnhanceEntity> results = populateEnhanceEntities(m, relationNames, client, query);
            return results != null && !results.isEmpty() ? results.get(0) : null;
        }
        else
        {
            Object o;
            try
            {
                o = client.find(m.getEntityClazz(), primaryKey);
            }
            catch (Exception e)
            {
                throw new PersistenceException(e);
            }
            return o != null ? new EnhanceEntity(o, getId(o, m), null) : null;
        }
    }

    /**
     * Checks if is string property.
     * 
     * @param m
     *            the m
     * @param fieldName
     *            the field name
     * @return true, if is string property
     */
    private boolean isStringProperty(EntityType entityType, String jpaColumnName, EntityMetadata m)
    {
        if (((AbstractAttribute) m.getIdAttribute()).getJPAColumnName().equals(jpaColumnName))
        {
            return ((AbstractAttribute) m.getIdAttribute()).getBindableJavaType().isAssignableFrom(String.class);
        }
        String fieldName = m.getFieldName(jpaColumnName);
        Attribute col = entityType.getAttribute(fieldName);
        // Column col = m.getColumn(fieldName);

        Field f = (Field) col.getJavaMember();
        return f != null ? ((AbstractAttribute) col).getBindableJavaType().isAssignableFrom(String.class) : false;
    }
}
