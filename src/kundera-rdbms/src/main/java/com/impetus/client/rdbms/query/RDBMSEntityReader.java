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
import java.sql.Date;
import java.util.ArrayList;
import java.util.Collection;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.rdbms.HibernateClient;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.persistence.AbstractEntityReader;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.query.KunderaQuery;
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
    public RDBMSEntityReader(String query, KunderaQuery kunderaQuery, final KunderaMetadata kunderaMetadata)
    {
        super(kunderaMetadata);
        this.jpaQuery = query;
        this.kunderaQuery = kunderaQuery;
    }

    /**
     * Instantiates a new rDBMS entity reader.
     */
    public RDBMSEntityReader(final KunderaMetadata kunderaMetadata)
    {
        super(kunderaMetadata);
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
            if (MetadataUtils.useSecondryIndex(((ClientBase) client).getClientMetadata()))
            {
                sqlQuery = getSqlQueryFromJPA(m, relationNames, null);
            }
            else
            {
                // prepare lucene query and find.
                Set<String> rSet = fetchDataFromLucene(m.getEntityClazz(), client);
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
            if (MetadataUtils.useSecondryIndex(((ClientBase) client).getClientMetadata()))
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
                    log.error("Error while executing handleAssociation for RDBMS, Caused by {}.", e);
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

        if (!result.isEmpty())
        {
            ls = new ArrayList<EnhanceEntity>(result.size());
            for (Object o : result)
            {
                EnhanceEntity entity = null;
                if (!o.getClass().isAssignableFrom(EnhanceEntity.class))
                {
                    entity = new EnhanceEntity(o, getId(o, m), null);
                }
                else
                {
                    entity = (EnhanceEntity) o;
                }
                ls.add(entity);
            }
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
        ApplicationMetadata appMetadata = kunderaMetadata.getApplicationMetadata();
        Metamodel metaModel = appMetadata.getMetamodel(entityMetadata.getPersistenceUnit());

        if (jpaQuery != null)
        {
            String query = appMetadata.getQuery(jpaQuery);
            boolean isNative = kunderaQuery != null ? kunderaQuery.isNative() : false;

            if (isNative)
            {
                return query != null ? query : jpaQuery;
            }
        }

        // Suffixing the UNDERSCORE instead of prefix as Oracle 11g complains
        // about invalid characters error while executing the request.
        String aliasName = entityMetadata.getTableName() + "_";

        StringBuilder queryBuilder = new StringBuilder("Select ");

        EntityType entityType = metaModel.entity(entityMetadata.getEntityClazz());
        Set<Attribute> attributes = entityType.getAttributes();
        for (Attribute field : attributes)
        {
            if (!field.isAssociation() && !field.isCollection()
                    && !((Field) field.getJavaMember()).isAnnotationPresent(ManyToMany.class)
                    && !((MetamodelImpl) metaModel).isEmbeddable(((AbstractAttribute) field).getBindableJavaType()))
            {
                queryBuilder.append(aliasName);
                queryBuilder.append(".");
                queryBuilder.append(((AbstractAttribute) field).getJPAColumnName());
                queryBuilder.append(", ");
            }
        }

        // Handle embedded columns, add them to list.
        Map<String, EmbeddableType> embeddedColumns = ((MetamodelImpl) metaModel).getEmbeddables(entityMetadata
                .getEntityClazz());
        for (EmbeddableType embeddedCol : embeddedColumns.values())
        {
            Set<Attribute> embeddedAttributes = embeddedCol.getAttributes();
            for (Attribute column : embeddedAttributes)
            {
                queryBuilder.append(aliasName);
                queryBuilder.append(".");
                queryBuilder.append(((AbstractAttribute) column).getJPAColumnName());
                queryBuilder.append(", ");
            }
        }

        if (relations != null)
        {
            for (String relation : relations)
            {
                Relation rel = entityMetadata.getRelation(entityMetadata.getFieldName(relation));
                String r = MetadataUtils.getMappedName(entityMetadata, rel, kunderaMetadata);
                if (!((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName().equalsIgnoreCase(
                        r != null ? r : relation)
                        && rel != null
                        && !rel.getProperty().isAnnotationPresent(ManyToMany.class)
                        && !rel.getProperty().isAnnotationPresent(OneToMany.class)
                        && (rel.getProperty().isAnnotationPresent(OneToOne.class)
                                && StringUtils.isBlank(rel.getMappedBy()) || rel.getProperty().isAnnotationPresent(
                                ManyToOne.class)))
                {
                    queryBuilder.append(aliasName);
                    queryBuilder.append(".");
                    queryBuilder.append(r != null ? r : relation);
                    queryBuilder.append(", ");
                }
            }
        }

        // Remove last ","
        queryBuilder.deleteCharAt(queryBuilder.lastIndexOf(","));

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
        }

        // Append conditions
        onCondition(entityMetadata, (MetamodelImpl) metaModel, primaryKeys, aliasName, queryBuilder, entityType);

        return queryBuilder.toString();
    }

    /**
     * 
     * @param entityMetadata
     * @param primaryKeys
     * @param aliasName
     * @param queryBuilder
     * @param entityType
     */
    private void onCondition(EntityMetadata entityMetadata, MetamodelImpl metamodel, Set<String> primaryKeys,
            String aliasName, StringBuilder queryBuilder, EntityType entityType)
    {
        if (primaryKeys == null)
        {
            for (Object o : conditions)
            {
                if (o instanceof FilterClause)
                {
                    FilterClause clause = ((FilterClause) o);
                    Object value = clause.getValue().get(0);
                    String propertyName = clause.getProperty();
                    String condition = clause.getCondition();

                    if (StringUtils.contains(propertyName, '.'))
                    {
                        int indexOf = propertyName.indexOf(".");
                        String jpaColumnName = propertyName.substring(0, indexOf);
                        String embeddedColumnName = propertyName.substring(indexOf + 1, propertyName.length());
                        String fieldName = entityMetadata.getFieldName(jpaColumnName);
                        Attribute attribute = entityType.getAttribute(fieldName);
                        EmbeddableType embeddedType = metamodel.embeddable(((AbstractAttribute) attribute)
                                .getBindableJavaType());
                        Attribute embeddedAttribute = embeddedType.getAttribute(embeddedColumnName);

                        addClause(entityMetadata, aliasName, queryBuilder, entityType, value, condition, fieldName,
                                embeddedAttribute);
                    }
                    else
                    {
                        String fieldName = entityMetadata.getFieldName(propertyName);
                        Attribute attribute = entityType.getAttribute(fieldName);
                        if (metamodel.isEmbeddable(((AbstractAttribute) attribute).getBindableJavaType()))
                        {
                            EmbeddableType embeddedType = metamodel.embeddable(((AbstractAttribute) attribute)
                                    .getBindableJavaType());
                            Set<Attribute> attributes = embeddedType.getAttributes();
                            for (Attribute embeddedAttribute : attributes)
                            {
                                Object embeddedAttributevalue = PropertyAccessorHelper.getObject(value,
                                        (Field) embeddedAttribute.getJavaMember());
                                addClause(entityMetadata, aliasName, queryBuilder, entityType, embeddedAttributevalue,
                                        condition, propertyName, embeddedAttribute);
                                queryBuilder.append(" and ");
                            }

                            queryBuilder.delete(queryBuilder.lastIndexOf("and"), queryBuilder.lastIndexOf("and") + 3);
                        }
                        else if (((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName().equals(
                                propertyName))
                        {
                            addClause(entityMetadata, aliasName, queryBuilder, entityType, value, condition,
                                    propertyName, entityMetadata.getIdAttribute());
                        }
                        else
                        {
                            addClause(entityMetadata, aliasName, queryBuilder, entityType, value, condition,
                                    propertyName, attribute);
                        }
                    }
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
            boolean isString = isStringProperty(entityType, col);
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
    }

    /**
     * 
     * @param entityMetadata
     * @param aliasName
     * @param queryBuilder
     * @param entityType
     * @param value
     * @param condition
     * @param propertyName
     * @param attribute
     */
    private void addClause(EntityMetadata entityMetadata, String aliasName, StringBuilder queryBuilder,
            EntityType entityType, Object value, String condition, String propertyName, Attribute attribute)
    {
        boolean isString = isStringProperty(entityType, attribute);

        // queryBuilder.append(aliasName);
        // queryBuilder.append(".");
        queryBuilder.append(StringUtils.replace(((AbstractAttribute) attribute).getJPAColumnName(), aliasName,
                aliasName));

        queryBuilder.append(" ");
        queryBuilder.append(condition);

        if (condition.equalsIgnoreCase("like"))
        {
            queryBuilder.append("%");
        }
        queryBuilder.append(" ");
        if (condition.equalsIgnoreCase("IN"))
        {
            buildINClause(queryBuilder, value, isString);
        }
        else
        {
            appendStringPrefix(queryBuilder, isString);
            queryBuilder.append(value);
            appendStringPrefix(queryBuilder, isString);
        }
    }

    /**
     * 
     * @param queryBuilder
     * @param value
     * @param isString
     */
    private void buildINClause(StringBuilder queryBuilder, Object value, boolean isString)
    {
        if (List.class.isAssignableFrom(value.getClass()) || Set.class.isAssignableFrom(value.getClass()))
        {
            queryBuilder.append(" (");
            Collection collection = ((Collection) value);
            for (Object obj : collection)
            {
                if (isString)
                {
                    appendStringPrefix(queryBuilder, isString);
                }
                queryBuilder.append(obj.toString());
                if (isString)
                {
                    appendStringPrefix(queryBuilder, isString);
                }
                queryBuilder.append(",");
            }
            if (!collection.isEmpty())
            {
                queryBuilder.deleteCharAt(queryBuilder.lastIndexOf(","));
            }
            queryBuilder.append(")");
        }
        else
        {
            queryBuilder.append(value.toString());
        }
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

        // return super.findById(primaryKey, m, client);
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
    private boolean isStringProperty(EntityType entityType, Attribute attribute)
    {
        String discriminatorColumn = ((AbstractManagedType) entityType).getDiscriminatorColumn();

        if (attribute.getName().equals(discriminatorColumn))
        {
            return true;
        }

        return attribute != null ? ((AbstractAttribute) attribute).getBindableJavaType().isAssignableFrom(String.class)
                || ((AbstractAttribute) attribute).getBindableJavaType().isAssignableFrom(Character.class)
                || ((AbstractAttribute) attribute).getBindableJavaType().isAssignableFrom(char.class)
                || ((AbstractAttribute) attribute).getBindableJavaType().isAssignableFrom(Date.class)
                || ((AbstractAttribute) attribute).getBindableJavaType().isAssignableFrom(java.util.Date.class) : false;
    }
}
