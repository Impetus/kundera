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
 * 
 */

package com.impetus.kundera.utils;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.persistence.PersistenceException;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.SingularAttribute;

import org.apache.commons.configuration.EnvironmentConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.proxy.ProxyHelper;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.KunderaQuery.FilterClause;
import com.impetus.kundera.query.LuceneQueryBuilder;
import com.impetus.kundera.query.QueryHandlerException;

public class KunderaCoreUtils
{

    private static final String COMPOSITE_KEY_SEPERATOR = "\001";

    private static final String LUCENE_COMPOSITE_KEY_SEPERATOR = "_";

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(KunderaCoreUtils.class);

    /**
     * Retrun map of external properties for given pu;
     * 
     * @param pu
     * @param externalProperties
     * @param persistenceUnits
     * @return
     */
    public static Map<String, Object> getExternalProperties(String pu, Map<String, Object> externalProperties,
            String... persistenceUnits)
    {
        Map<String, Object> puProperty;
        if (persistenceUnits != null && persistenceUnits.length > 1 && externalProperties != null)
        {
            puProperty = (Map<String, Object>) externalProperties.get(pu);

            // if property found then return it, if it is null by pass it, else
            // throw invalidConfiguration.
            if (puProperty != null)
            {
                return fetchPropertyMap(puProperty);
            }
            return null;
        }
        return externalProperties;
    }

    /**
     * @param puProperty
     * @return
     */
    private static Map<String, Object> fetchPropertyMap(Map<String, Object> puProperty)
    {
        if (puProperty.getClass().isAssignableFrom(Map.class) || puProperty instanceof Map)
        {
            return puProperty;
        }
        else
        {
            throw new InvalidConfigurationException(
                    "For cross data store persistence, please specify as: Map {pu,Map of properties}");
        }
    }

    public static boolean isEmptyOrNull(Object o)
    {
        if (o == null)
        {
            return true;
        }

        if (!ProxyHelper.isProxyOrCollection(o))
        {
            if (PropertyAccessorHelper.isCollection(o.getClass()))
            {
                Collection c = (Collection) o;
                if (c.isEmpty())
                {
                    return true;
                }
            }
            else if (Map.class.isAssignableFrom(o.getClass()))
            {
                Map m = (Map) o;
                if (m.isEmpty())
                {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Prepares composite key .
     * 
     * @param m
     *            entity metadata
     * @param compositeKey
     *            composite key instance
     * @return redis key
     */
    public static String prepareCompositeKey(final EntityMetadata m, final Object compositeKey)
    {
        Field[] fields = m.getIdAttribute().getBindableJavaType().getDeclaredFields();

        StringBuilder stringBuilder = new StringBuilder();
        for (Field f : fields)
        {
            if (!ReflectUtils.isTransientOrStatic(f))
            {
                try
                {
                    String fieldValue = PropertyAccessorHelper.getString(compositeKey, f);

                    // what if field value is null????
                    stringBuilder.append(fieldValue);
                    stringBuilder.append(COMPOSITE_KEY_SEPERATOR);
                }
                catch (IllegalArgumentException e)
                {
                    logger.error("Error during prepare composite key, Caused by {}.", e);
                    throw new PersistenceException(e);
                }
            }
        }

        if (stringBuilder.length() > 0)
        {
            stringBuilder.deleteCharAt(stringBuilder.lastIndexOf(COMPOSITE_KEY_SEPERATOR));
        }
        return stringBuilder.toString();
    }

    /**
     * Prepares composite key as a lucene key.
     * 
     * @param m
     *            entity metadata
     * @param metaModel
     *            meta model.
     * @param compositeKey
     *            composite key instance
     * @return redis key
     */
    public static String prepareCompositeKey(final SingularAttribute attribute, final MetamodelImpl metaModel,
            final Object compositeKey)
    {
        Field[] fields = attribute.getBindableJavaType().getDeclaredFields();
        EmbeddableType embeddable = metaModel.embeddable(attribute.getBindableJavaType());
        StringBuilder stringBuilder = new StringBuilder();

        try
        {
            for (Field f : fields)
            {
                if (!ReflectUtils.isTransientOrStatic(f))
                {
                    if (metaModel.isEmbeddable(((AbstractAttribute) embeddable.getAttribute(f.getName()))
                            .getBindableJavaType()))
                    {
                        f.setAccessible(true);
                        stringBuilder.append(
                                prepareCompositeKey((SingularAttribute) embeddable.getAttribute(f.getName()),
                                        metaModel, f.get(compositeKey))).append(LUCENE_COMPOSITE_KEY_SEPERATOR);
                    }
                    else
                    {
                        String fieldValue = PropertyAccessorHelper.getString(compositeKey, f);
                        fieldValue = fieldValue.replaceAll("[^a-zA-Z0-9]", "_");

                        stringBuilder.append(fieldValue);
                        stringBuilder.append(LUCENE_COMPOSITE_KEY_SEPERATOR);
                    }
                }
            }
        }
        catch (IllegalAccessException e)
        {
            logger.error(e.getMessage());
        }
        catch (IllegalArgumentException e)
        {
            logger.error("Error during prepare composite key, Caused by {}.", e);
            throw new PersistenceException(e);
        }

        if (stringBuilder.length() > 0)
        {
            stringBuilder.deleteCharAt(stringBuilder.lastIndexOf(LUCENE_COMPOSITE_KEY_SEPERATOR));
        }
        return stringBuilder.toString();
    }

    /**
     * Resolves variable in path given as string
     * 
     * @param input
     *            String input url Code inspired by
     *            :http://stackoverflow.com/questions/2263929/
     *            regarding-application-properties-file-and-environment-variable
     */
    public static String resolvePath(String input)
    {
        if (null == input)
        {
            return input;
        }

        // matching for 2 groups match ${VAR_NAME} or $VAR_NAME
        Pattern pathPattern = Pattern.compile("\\$\\{(.+?)\\}");
        Matcher matcherPattern = pathPattern.matcher(input); // get a matcher
                                                             // object
        StringBuffer sb = new StringBuffer();
        EnvironmentConfiguration config = new EnvironmentConfiguration();
        SystemConfiguration sysConfig = new SystemConfiguration();

        while (matcherPattern.find())
        {

            String confVarName = matcherPattern.group(1) != null ? matcherPattern.group(1) : matcherPattern.group(2);
            String envConfVarValue = config.getString(confVarName);
            String sysVarValue = sysConfig.getString(confVarName);

            if (envConfVarValue != null)
            {

                matcherPattern.appendReplacement(sb, envConfVarValue);

            }
            else if (sysVarValue != null)
            {

                matcherPattern.appendReplacement(sb, sysVarValue);

            }
            else
            {
                matcherPattern.appendReplacement(sb, "");
            }
        }
        matcherPattern.appendTail(sb);
        return sb.toString();
    }

    public static int countNonSyntheticFields(Class<?> clazz)
    {
        int count = 0;
        for (Field f : clazz.getDeclaredFields())
        {
            if (!f.isSynthetic() || !ReflectUtils.isTransientOrStatic(f))
            {
                count++;
            }
        }

        return count;
    }

    /**
     * Gets the lucene query from jpa query.
     * 
     * @return the lucene query from jpa query
     */
    public static String getLuceneQueryFromJPAQuery(final KunderaQuery kunderaQuery,
            final KunderaMetadata kunderaMetadata)
    {

        LuceneQueryBuilder queryBuilder = new LuceneQueryBuilder();
        EntityMetadata metadata = kunderaQuery.getEntityMetadata();
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                metadata.getPersistenceUnit());
        Class valueClazz = null;
        EntityType entity = metaModel.entity(metadata.getEntityClazz());
        boolean partitionKeyCheck = true;

        for (Object object : kunderaQuery.getFilterClauseQueue())
        {
            if (object instanceof FilterClause)
            {
                FilterClause filter = (FilterClause) object;
                String property = filter.getProperty();
                String condition = filter.getCondition();
                String valueAsString = filter.getValue().get(0).toString();
                String fieldName = metadata.getFieldName(property);
                boolean isEmbeddedId = metaModel.isEmbeddable(metadata.getIdAttribute().getBindableJavaType());
                String idColumn = ((AbstractAttribute) metadata.getIdAttribute()).getJPAColumnName();
                valueClazz = getValueType(entity, fieldName);

                if (isEmbeddedId)
                {
                    if (idColumn.equals(property))
                    {
                        valueAsString = prepareCompositeKey(metadata.getIdAttribute(), metaModel, filter.getValue()
                                .get(0));
                        queryBuilder.appendIndexName(metadata.getIndexName()).appendPropertyName(idColumn)
                                .buildQuery(condition, valueAsString, valueClazz);
                    }
                    else
                    {
                        valueClazz = metadata.getIdAttribute().getBindableJavaType();
                        if (property.lastIndexOf('.') != property.indexOf('.') && partitionKeyCheck)
                        {
                            isCompletePartitionKeyPresentInQuery(kunderaQuery.getFilterClauseQueue(), metaModel,
                                    metadata);
                            partitionKeyCheck = false;
                        }

                        if (metaModel.isEmbeddable(filter.getValue().get(0).getClass()))
                        {
                            prepareLuceneQueryForPartitionKey(queryBuilder, filter.getValue().get(0), metaModel,
                                    metadata.getIndexName(), valueClazz);
                        }
                        else
                        {
                            property = property.substring(property.lastIndexOf(".") + 1);
                            queryBuilder.appendIndexName(metadata.getIndexName())
                                    .appendPropertyName(getPropertyName(metadata, property, kunderaMetadata))
                                    .buildQuery(condition, valueAsString, valueClazz);
                        }
                    }
                }
                else
                {
                    queryBuilder.appendIndexName(metadata.getIndexName())
                            .appendPropertyName(getPropertyName(metadata, property, kunderaMetadata))
                            .buildQuery(condition, valueAsString, valueClazz);
                }
            }
            else
            {
                queryBuilder.buildQuery(object.toString(), object.toString(), String.class);
            }
        }

        queryBuilder.appendEntityName(kunderaQuery.getEntityClass().getCanonicalName().toLowerCase());
        return queryBuilder.getQuery();
    }

    /**
     * cheking whether all the fields of partition key are present in the jpa
     * query
     * 
     * @param filterQueue
     * @param metaModel
     * @param metadata
     */
    private static void isCompletePartitionKeyPresentInQuery(Queue filterQueue, MetamodelImpl metaModel,
            EntityMetadata metadata)
    {
        Set<String> partitionKeyFields = new HashSet<String>();
        populateEmbeddedIdFields(metaModel.embeddable(metadata.getIdAttribute().getBindableJavaType()).getAttributes(),
                metaModel, partitionKeyFields);

        Set<String> queryAttributes = new HashSet<String>();
        for (Object object : filterQueue)
        {
            if (object instanceof FilterClause)
            {
                FilterClause filter = (FilterClause) object;
                String property = filter.getProperty();
                String filterAttr[] = property.split("\\.");
                for (String s : filterAttr)
                {
                    queryAttributes.add(s);
                }
            }
        }
        if (!queryAttributes.containsAll(partitionKeyFields))
        {
            throw new QueryHandlerException("Incomplete partition key fields in query");
        }
    }

    /**
     * recursively populate all the fields present in partition key
     * 
     * @param embeddedAttributes
     * @param metaModel
     * @param embeddedIdFields
     */
    private static void populateEmbeddedIdFields(Set<Attribute> embeddedAttributes, MetamodelImpl metaModel,
            Set<String> embeddedIdFields)
    {
        for (Attribute attribute : embeddedAttributes)
        {
            if (!ReflectUtils.isTransientOrStatic((Field) attribute.getJavaMember()))
            {
                if (metaModel.isEmbeddable(attribute.getJavaType()))
                {
                    EmbeddableType embeddable = metaModel.embeddable(attribute.getJavaType());
                    populateEmbeddedIdFieldsUtil(embeddable.getAttributes(), metaModel, embeddedIdFields);
                }
            }
        }
    }

    private static void populateEmbeddedIdFieldsUtil(Set<Attribute> embeddedAttributes, MetamodelImpl metaModel,
            Set<String> embeddedIdFields)
    {
        for (Attribute attribute : embeddedAttributes)
        {
            if (!ReflectUtils.isTransientOrStatic((Field) attribute.getJavaMember()))
            {
                if (metaModel.isEmbeddable(attribute.getJavaType()))
                {
                    EmbeddableType embeddable = metaModel.embeddable(attribute.getJavaType());
                    populateEmbeddedIdFieldsUtil(embeddable.getAttributes(), metaModel, embeddedIdFields);
                }
                else
                {
                    String columnName = ((AbstractAttribute) attribute).getJPAColumnName();
                    embeddedIdFields.add(columnName);
                }
            }
        }
    }

    private static void prepareLuceneQueryForPartitionKey(LuceneQueryBuilder queryBuilder, Object key,
            MetamodelImpl metaModel, String indexName, Class valueClazz)
    {
        Field[] fields = key.getClass().getDeclaredFields();
        EmbeddableType embeddable = metaModel.embeddable(key.getClass());
        boolean appendAnd = false;

        try
        {
            for (int i = 0; i < fields.length; i++)
            {
                if (!ReflectUtils.isTransientOrStatic(fields[i]))
                {
                    if (metaModel.isEmbeddable(((AbstractAttribute) embeddable.getAttribute(fields[i].getName()))
                            .getBindableJavaType()))
                    {
                        fields[i].setAccessible(true);
                        prepareLuceneQueryForPartitionKey(queryBuilder, fields[i].get(key), metaModel, indexName,
                                valueClazz);
                    }
                    else
                    {
                        if (appendAnd)
                        {
                            queryBuilder.buildQuery("AND", "AND", String.class);
                        }
                        appendAnd = true;
                        String fieldValue = PropertyAccessorHelper.getString(key, fields[i]);
                        fieldValue = fieldValue.replaceAll("[^a-zA-Z0-9]", "_");
                        queryBuilder.appendIndexName(indexName).appendPropertyName(fields[i].getName())
                                .buildQuery("=", fieldValue, valueClazz);
                    }
                }
            }
        }
        catch (IllegalArgumentException e)
        {
            logger.error("Error during prepare composite key, Caused by {}.", e);
            throw new PersistenceException(e);
        }
        catch (IllegalAccessException e)
        {
            logger.error(e.getMessage());
        }
    }

    private static Class getValueType(EntityType entity, String fieldName)
    {
        Class valueClazz = null;
        if (fieldName != null)
        {
            valueClazz = ((AbstractAttribute) entity.getAttribute(fieldName)).getBindableJavaType();
        }
        return valueClazz;
    }

    private static String getPropertyName(final EntityMetadata metadata, final String property,
            final KunderaMetadata kunderaMetadata)
    {
        if (MetadataUtils.getEnclosingEmbeddedFieldName(metadata, property, true, kunderaMetadata) != null)
        {
            return property.substring(property.indexOf(".") + 1, property.length());
        }

        return property;
    }

    public static boolean isShowQueryEnabled(final Map<String, Object> properties, final String persistenceUnit,
            final KunderaMetadata kunderaMetadata)
    {
        boolean showQuery = false;
        showQuery = properties != null ? Boolean.parseBoolean((String) properties
                .get(PersistenceProperties.KUNDERA_SHOW_QUERY)) : false;
        if (!showQuery)
        {
            showQuery = persistenceUnit != null ? Boolean.parseBoolean(kunderaMetadata.getApplicationMetadata()
                    .getPersistenceUnitMetadata(persistenceUnit).getProperties()
                    .getProperty(PersistenceProperties.KUNDERA_SHOW_QUERY)) : false;
        }
        return showQuery;
    }

    public static void printQuery(String query, boolean showQuery)
    {
        if (showQuery)
        {
            System.out.println(query);
        }
    }

    public static void printQueryWithFilterClause(Queue filterClausequeue, String tableName)
    {
        StringBuilder printQuery = new StringBuilder("Fetch data from ").append(tableName).append(" for ");
        for (Object clause : filterClausequeue)
        {
            if (clause instanceof FilterClause)
            {
                printQuery.append(((FilterClause) clause).getProperty()).append(" ")
                        .append(((FilterClause) clause).getCondition()).append(" ")
                        .append(((FilterClause) clause).getValue());
            }
            else
            {
                printQuery.append(" ").append(clause.toString()).append(" ");
            }
        }
        KunderaCoreUtils.printQuery(printQuery.toString(), true);
    }

    public static Object getEntity(Object e)
    {
        if (e != null)
        {
            return e.getClass().isAssignableFrom(EnhanceEntity.class) ? ((EnhanceEntity) e).getEntity() : e;
        }
        return null;
    }

    /**
     * Initialize.
     * 
     * @param tr
     *            the tr
     * @param m
     *            the m
     * @param entity
     *            the entity
     * @param tr
     * @return the object
     * @throws InstantiationException
     *             the instantiation exception
     * @throws IllegalAccessException
     *             the illegal access exception
     */
    public static Object initialize(EntityMetadata m, Object entity, Object id)
    {
        try
        {
            if (entity == null)
            {
                entity = createNewInstance(m.getEntityClazz());
            }
            if (id != null)
            {
                PropertyAccessorHelper.setId(entity, m, id);
            }
            return entity;
        }
        catch (Exception e)
        {
            throw new PersistenceException("Error occured while instantiating entity.", e);
        }
    }

    /**
     * Initialize.
     * 
     * @param tr
     *            the tr
     * @param m
     *            the m
     * @param entity
     *            the entity
     * @param tr
     * @return the object
     * @throws InstantiationException
     *             the instantiation exception
     * @throws IllegalAccessException
     *             the illegal access exception
     */
    public static Object initialize(Class clazz, Object record)
    {
        try
        {
            if (record == null)
            {
                record = createNewInstance(clazz);
            }
            return record;
        }
        catch (Exception e)
        {
            throw new PersistenceException("Error occured while instantiating entity.", e);
        }
    }

    /**
     * @param clazz
     * @return
     */
    public static Object createNewInstance(Class clazz)
    {
        Object target = null;
        try
        {
            Constructor[] constructors = clazz.getDeclaredConstructors();
            for (Constructor constructor : constructors)
            {
                if ((Modifier.isProtected(constructor.getModifiers()) || Modifier.isPublic(constructor.getModifiers()))
                        && constructor.getParameterTypes().length == 0)
                {
                    constructor.setAccessible(true);
                    target = constructor.newInstance();
                    constructor.setAccessible(false);
                    break;
                }
            }
            return target;

        }
        catch (InstantiationException iex)
        {
            logger.error("Error while creating an instance of {} .", clazz);
            throw new PersistenceException(iex);
        }

        catch (IllegalAccessException iaex)
        {
            logger.error("Illegal Access while reading data from {}, Caused by: .", clazz, iaex);
            throw new PersistenceException(iaex);
        }

        catch (Exception e)
        {
            logger.error("Error while creating an instance of {}, Caused by: .", clazz, e);
            throw new PersistenceException(e);
        }
    }

    /**
     * Gets the JPA column name.
     * 
     * @param field
     *            the field
     * @param entityMetadata
     *            the entity metadata
     * @param metaModel
     *            the meta model
     * @return the JPA column name
     */
    public static String getJPAColumnName(String field, EntityMetadata entityMetadata, MetamodelImpl metaModel)
    {
        if (field.indexOf('.') > 0)
        {
            return ((AbstractAttribute) metaModel.entity(entityMetadata.getEntityClazz()).getAttribute(
                    field.substring(field.indexOf('.') + 1,
                            field.indexOf(')') > 0 ? field.indexOf(')') : field.length()))).getJPAColumnName();
        }
        else
        {
            return ((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName();
        }
    }

}
