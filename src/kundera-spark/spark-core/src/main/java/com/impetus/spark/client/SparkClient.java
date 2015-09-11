/*******************************************************************************
 *  * Copyright 2015 Impetus Infotech.
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
package com.impetus.spark.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.client.ClientPropertiesSetter;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.generator.Generator;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.spark.constants.SparkQueryConstants;
import com.impetus.spark.datahandler.SparkDataHandler;
import com.impetus.spark.query.SparkQuery;

/**
 * The Class SparkClient.
 * 
 * @author: karthikp.manchala
 */
public class SparkClient extends ClientBase implements Client<SparkQuery>, ClientPropertiesSetter
{

    /** The Constant DATA_CLIENT. */
    private static final String DATA_CLIENT = "kundera.client";

    /** The spark context. */
    final SparkContext sparkContext;

    /** The sql context. */
    final HiveContext sqlContext;

    /** The properties. */
    Map<String, Object> properties = new HashMap<String, Object>();

    /** The reader. */
    private EntityReader reader;

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(SparkClient.class);

    /** The registered tables. */
    private static Map<String, Boolean> registeredTables = new HashMap<String, Boolean>();

    /** The data handler. */
    private SparkDataHandler dataHandler = new SparkDataHandler(kunderaMetadata);

    /**
     * Instantiates a new spark client.
     * 
     * @param kunderaMetadata
     *            the kundera metadata
     * @param reader
     *            the reader
     * @param properties
     *            the properties
     * @param persistenceUnit
     *            the persistence unit
     * @param sparkContext
     *            the spark context
     * @param sqlContext
     *            the sql context
     */
    protected SparkClient(KunderaMetadata kunderaMetadata, EntityReader reader, Map<String, Object> properties,
            String persistenceUnit, SparkContext sparkContext, HiveContext sqlContext)
    {
        super(kunderaMetadata, properties, persistenceUnit);
        this.reader = reader;
        this.sparkContext = sparkContext;
        this.sqlContext = sqlContext;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class,
     * java.lang.Object)
     */
    public Object find(Class entityClass, Object key)
    {

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);

        String tableName = entityMetadata.getTableName();
        Class fieldClass = entityMetadata.getIdAttribute().getJavaType();
        String select_Query = SparkQueryConstants.SELECTALL_QUERY;
        select_Query = StringUtils.replace(select_Query, SparkQueryConstants.TABLE, tableName);
        StringBuilder builder = new StringBuilder(select_Query);
        builder.append(SparkQueryConstants.ADD_WHERE_CLAUSE);
        builder.append(dataHandler.getColumnName(entityMetadata.getIdAttribute()));
        builder.append(SparkQueryConstants.EQUALS);

        appendValue(builder, fieldClass, key);
        List result = this.executeQuery(builder.toString(), entityMetadata, null);
        return result.isEmpty() ? null : result.get(0);
    }

    /**
     * Append value.
     * 
     * @param builder
     *            the builder
     * @param fieldClazz
     *            the field clazz
     * @param value
     *            the value
     */
    private void appendValue(StringBuilder builder, Class fieldClazz, Object value)
    {

        if (fieldClazz != null
                && value != null
                && (fieldClazz.isAssignableFrom(String.class) || fieldClazz.isAssignableFrom(char.class)
                        || fieldClazz.isAssignableFrom(Character.class) || value instanceof Enum))
        {

            if (fieldClazz.isAssignableFrom(String.class))
            {
                // To allow escape character
                value = ((String) value).replaceAll("'", "''");
            }
            builder.append("'");

            if (value instanceof Enum)
            {
                builder.append(((Enum) value).name());
            }

            else
            {
                builder.append(value);
            }
            builder.append("'");
        }
        else
        {
            builder.append(value);
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#close()
     */
    public void close()
    {
        registeredTables.clear();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.client.Client#persistJoinTable(com.impetus.kundera
     * .persistence.context.jointable.JoinTableData)
     */
    public void persistJoinTable(JoinTableData joinTableData)
    {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#findIdsByColumn(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.String, java.lang.Object,
     * java.lang.Class)
     */
    public Object[] findIdsByColumn(String schemaName, String tableName, String pKeyName, String columnName,
            Object columnValue, Class entityClazz)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#deleteByColumn(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.Object)
     */
    public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue)
    {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#findByRelation(java.lang.String,
     * java.lang.Object, java.lang.Class)
     */
    public List findByRelation(String colName, Object colValue, Class entityClazz)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getReader()
     */
    public EntityReader getReader()
    {
        return reader;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getQueryImplementor()
     */
    public Class getQueryImplementor()
    {
        // TODO Auto-generated method stub
        return SparkQuery.class;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getIdGenerator()
     */
    public Generator getIdGenerator()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.client.ClientBase#onPersist(com.impetus.kundera.metadata
     * .model.EntityMetadata, java.lang.Object, java.lang.Object,
     * java.util.List)
     */
    @Override
    protected void onPersist(EntityMetadata entityMetadata, Object entity, Object id, List<RelationHolder> rlHolders)
    {
        // TODO Auto-generated method stub
        // handle persist object
        PersistenceUnitMetadata puMetadata = kunderaMetadata.getApplicationMetadata().getPersistenceUnitMetadata(
                persistenceUnit);
        String clientName = puMetadata.getProperty(DATA_CLIENT).toLowerCase();

        List listEntity = new ArrayList<>();
        listEntity.add(entity);

        SparkDataClient dataClient = SparkDataClientFactory.getDataClient(clientName);

        dataClient.persist(listEntity, entityMetadata, this);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.ClientBase#delete(java.lang.Object,
     * java.lang.Object)
     */
    @Override
    protected void delete(Object entity, Object pKey)
    {
        // TODO Auto-generated method stub

    }

    /**
     * Execute query.
     * 
     * @param query
     *            the query
     * @param m
     *            the m
     * @param kunderaQuery
     *            the kundera query
     * @return the list
     */
    public List executeQuery(String query, EntityMetadata m, KunderaQuery kunderaQuery)
    {
        DataFrame dataFrame = getDataFrame(query, m, kunderaQuery);

        // dataFrame.show();
        return dataHandler.loadDataAndPopulateResults(dataFrame, m, kunderaQuery);
    }

    /**
     * Gets the data frame.
     * 
     * @param query
     *            the query
     * @param m
     *            the m
     * @param kunderaQuery
     *            the kundera query
     * @return the data frame
     */
    public DataFrame getDataFrame(String query, EntityMetadata m, KunderaQuery kunderaQuery)
    {
        PersistenceUnitMetadata puMetadata = kunderaMetadata.getApplicationMetadata().getPersistenceUnitMetadata(
                persistenceUnit);
        String clientName = puMetadata.getProperty(DATA_CLIENT).toLowerCase();

        SparkDataClient dataClient = SparkDataClientFactory.getDataClient(clientName);
        if (registeredTables.get(m.getTableName()) == null || !registeredTables.get(m.getTableName()))
        {
            dataClient.registerTable(m, this);
            registeredTables.put(m.getTableName(), true);
        }
        // at this level temp table or table should be ready
        DataFrame dataFrame = sqlContext.sql(query);

        return dataFrame;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.client.ClientPropertiesSetter#populateClientProperties
     * (com.impetus.kundera.client.Client, java.util.Map)
     */
    @Override
    public void populateClientProperties(Client client, Map<String, Object> properties)
    {
        this.properties.putAll(properties);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#findAll(java.lang.Class,
     * java.lang.String[], java.lang.Object[])
     */
    @Override
    public <E> List<E> findAll(Class<E> entityClass, String[] columnsToSelect, Object... keys)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class,
     * java.util.Map)
     */
    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getColumnsById(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.String, java.lang.Object,
     * java.lang.Class)
     */
    @Override
    public <E> List<E> getColumnsById(String schemaName, String tableName, String pKeyColumnName, String columnName,
            Object pKeyColumnValue, Class columnJavaType)
    {
        // TODO Auto-generated method stub
        return null;
    }

}
