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
package com.impetus.client.hbase.query;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Query;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.impetus.client.hbase.HBaseClient;
import com.impetus.client.hbase.HBaseEntityReader;
import com.impetus.client.hbase.utils.HBaseUtils;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.KunderaQuery.FilterClause;
import com.impetus.kundera.query.QueryHandlerException;
import com.impetus.kundera.query.QueryImpl;

/**
 * Query implementation for HBase, translates JPQL into HBase Filters using
 * {@link QueryTranslator}.
 * 
 * @author vivek.mishra
 * 
 */
public class HBaseQuery extends QueryImpl implements Query
{

    /** the log used by this class. */
    private static Log log = LogFactory.getLog(HBaseQuery.class);

    /**
     * Holds reference to entity reader.
     */
    private EntityReader reader = new HBaseEntityReader();

    /**
     * Constructor using fields.
     * 
     * @param query
     *            jpa query.
     * @param persistenceDelegator
     *            persistence delegator interface.
     */
    public HBaseQuery(String query, KunderaQuery kunderaQuery, PersistenceDelegator persistenceDelegator)
    {
        super(query, persistenceDelegator);
        this.kunderaQuery = kunderaQuery;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.query.QueryImpl#populateEntities(com.impetus.kundera
     * .metadata.model.EntityMetadata, com.impetus.kundera.client.Client)
     */
    @Override
    protected List<Object> populateEntities(EntityMetadata m, Client client)
    {
        List results = onQuery(m, client);
        ((HBaseClient) client).setFilter(null);
        return results;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.query.QueryImpl#recursivelyPopulateEntities(com.impetus
     * .kundera.metadata.model.EntityMetadata,
     * com.impetus.kundera.client.Client)
     */
    @Override
    protected List<Object> recursivelyPopulateEntities(EntityMetadata m, Client client)
    {
        // required in case of associated entities.
        List ls = onQuery(m, client);
        ((HBaseClient) client).setFilter(null);
        return setRelationEntities(ls, client, m);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#getReader()
     */
    @Override
    protected EntityReader getReader()
    {
        return reader;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#onExecuteUpdate()
     */
    @Override
    protected int onExecuteUpdate()
    {
        if (kunderaQuery.isDeleteUpdate())
        {
            List result = getResultList();
            return result != null ? result.size() : 0;
        }

        return 0;
    }

    /**
     * Parses and translates query into HBase filter and invokes client's method
     * to return list of entities.
     * 
     * @param m
     *            Entity metadata
     * @param client
     *            hbase client
     * @return list of entities.
     */
    private List onQuery(EntityMetadata m, Client client)
    {
        // Called only in case of standalone entity.
        QueryTranslator translator = new QueryTranslator();
        translator.translate(getKunderaQuery(), m);
        String[] columns = getTranslatedColumns(m, getKunderaQuery().getResult());
        Map<Boolean, Filter> filter = translator.getFilter();
        if (translator.isFindById && (filter == null && columns == null))
        {
            List results = new ArrayList();

            Object output = client.find(m.getEntityClazz(), translator.rowKey);
            if (output != null)
            {
                results.add(output);
                return results;
            }
        }
        if (translator.isFindById && filter == null && columns != null)
        {
            return ((HBaseClient) client).findByRange(m.getEntityClazz(), m, translator.rowKey, translator.rowKey,
                    columns);
        }
        if (MetadataUtils.useSecondryIndex(m.getPersistenceUnit()))
        {
            if (filter == null && !translator.isFindById)
            {
                // means complete scan without where clause, scan all records.
                // findAll.
                if (translator.isRangeScan())
                {
                    return ((HBaseClient) client).findByRange(m.getEntityClazz(), m, translator.getStartRow(),
                            translator.getEndRow(), columns);
                }
                else
                {
                    return ((HBaseClient) client).findByRange(m.getEntityClazz(), m, null, null, columns);
                }
            }
            else
            {
                // means WHERE clause is present.

                if (filter != null && filter.values() != null && !filter.values().isEmpty())
                {
                    ((HBaseClient) client).setFilter(filter.values().iterator().next());
                }
                if (translator.isRangeScan())
                {
                    return ((HBaseClient) client).findByRange(m.getEntityClazz(), m, translator.getStartRow(),
                            translator.getEndRow(), columns);
                }
                else
                {
                    // if range query. means query over id column. create range
                    // scan method.

                    // else setFilter to client and invoke new method. find by
                    // query if isFindById is false! else invoke findById
                    return ((HBaseClient) client).findByQuery(m.getEntityClazz(), m, columns);
                }
            }
        }
        else
        {
            List results = null;
            return populateUsingLucene(m, client, results);
        }
        // return null;
    }

    /**
     * @param columns
     * @param m
     * @return
     */
    private String[] getTranslatedColumns(EntityMetadata m, String[] columns)
    {
        if (columns != null)
        {
            String[] translatedColumns = new String[columns.length - 1];
            MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                    m.getPersistenceUnit());

            EntityType entity = metaModel.entity(m.getEntityClazz());
            int count = 0;
            for (int i = 1; i < columns.length; i++)
            {
                if (columns[i] != null)
                {
                    Attribute col = entity.getAttribute(columns[i]);
                    if (col == null)
                    {
                        throw new QueryHandlerException("column type is null for: " + columns[i]);
                    }
                    translatedColumns[count++] = ((AbstractAttribute) col).getJPAColumnName();
                }
            }
            return translatedColumns;
        }
        return null;
    }

    /**
     * Query translator to translate JPQL into HBase query definition(e.g.
     * Filter/Filterlist)
     * 
     * @author vivek.mishra
     * 
     */
    class QueryTranslator
    {
        /* filter list to hold collection for applied filters */
        private List<Filter> filterList;

        /* Returns true, if intended for id column */
        private boolean isIdColumn;

        /*
         * byte[] value for start row, in case of range query, else will contain
         * null.
         */
        private byte[] startRow;

        /*
         * byte[] value for end row, in case of range query, else will contain
         * null.
         */
        private byte[] endRow;

        /* is true, if query intended for row key equality. */
        private boolean isFindById;

        /* row key value. */
        byte[] rowKey;

        /**
         * Translates kundera query into collection of to be applied HBase
         * filter/s.
         * 
         * @param query
         *            kundera query.
         * @param m
         *            entity's metadata.
         */
        void translate(KunderaQuery query, EntityMetadata m)
        {
            String idColumn = ((AbstractAttribute) m.getIdAttribute()).getJPAColumnName();
            for (Object obj : query.getFilterClauseQueue())
            {
                boolean isIdColumn = false;
                // parse for filter(e.g. where) clause.

                if (obj instanceof FilterClause)
                {
                    String condition = ((FilterClause) obj).getCondition();
                    String name = ((FilterClause) obj).getProperty();
                    Object value = ((FilterClause) obj).getValue();
                    if (/*(!isIdColumn) || */idColumn.equalsIgnoreCase(name))
                    {
                        isIdColumn = true;
                    }

                    onParseFilter(condition, name, value, isIdColumn, m);
                }
                else
                {
                    // Case of AND and OR clause.
                    String opr = obj.toString();
                    if (opr.equalsIgnoreCase("or"))
                    {
                        log.error("Support for OR clause is not enabled with in Hbase");
                        throw new QueryHandlerException("unsupported clause " + opr + " for Hbase");
                    }
                }

            }
        }

        /**
         * Returns collection of parsed filter.
         * 
         * @return map.
         */
        Map<Boolean, Filter> getFilter()
        {
            if (filterList != null)
            {
                Map<Boolean, Filter> queryClause = new HashMap<Boolean, Filter>();
                queryClause.put(isIdColumn, new FilterList(filterList));
                return queryClause;
            }

            return null;
        }

        /**
         * On parsing filter clause(e.g. WHERE clause).
         * 
         * @param condition
         *            condition
         * @param name
         *            column name.
         * @param value
         *            column value.
         * @param isIdColumn
         *            if it is an id column.
         * @param m
         *            entity metadata.
         */
        private void onParseFilter(String condition, String name, Object value, boolean isIdColumn, EntityMetadata m)
        {
            CompareOp operator = HBaseUtils.getOperator(condition, isIdColumn);

            byte[] valueInBytes = getBytes(name, m, value);
            if (!isIdColumn)
            {
                // Filter f = new SingleColumnValueFilter(name.getBytes(),
                // name.getBytes(), operator, valueInBytes);
                Filter f = new SingleColumnValueFilter(Bytes.toBytes(m.getTableName()), Bytes.toBytes(name), operator,
                        valueInBytes);
                addToFilter(f);
            }
            else
            {
                if (operator.equals(CompareOp.GREATER_OR_EQUAL) || operator.equals(CompareOp.GREATER))
                {
                    startRow = valueInBytes;
                }
                else if (operator.equals(CompareOp.LESS_OR_EQUAL) || operator.equals(CompareOp.LESS))
                {
                    endRow = valueInBytes;
                }
                else if (operator.equals(CompareOp.EQUAL))
                {
                    rowKey = getBytes(m.getIdAttribute().getName(), m, value);
                    // startRow = getBytes(m.getIdAttribute().getName(), m,
                    // value);
                    endRow = null;
                    isFindById = true;
                }
            }
            this.isIdColumn = isIdColumn;
        }

        /**
         * @return the startRow
         */
        byte[] getStartRow()
        {
            return startRow;
        }

        /**
         * @return the endRow
         */
        byte[] getEndRow()
        {
            return endRow;
        }

        /**
         * @return the isFindById
         */
        boolean isFindById()
        {
            return isFindById;
        }

        boolean isRangeScan()
        {
            return startRow != null || endRow != null && !isFindById;
        }

        /**
         * @param f
         */
        private void addToFilter(Filter f)
        {
            if (filterList == null)
            {
                filterList = new ArrayList<Filter>();

            }
            filterList.add(f);

        }
    }

    private byte[] getBytes(String jpaFieldName, EntityMetadata m, Object value)
    {
        Attribute idCol = m.getIdAttribute();
        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());

        EntityType entity = metaModel.entity(m.getEntityClazz());
        Field f = null;
        boolean isId = false;
        if (idCol.getName().equals(jpaFieldName))
        {
            f = (Field) idCol.getJavaMember();
            isId = true;
        }
        else
        {
            String fieldName = m.getFieldName(jpaFieldName);
            Attribute col = entity.getAttribute(fieldName);
            // Column col = m.getColumn(jpaFieldName);
            if (col == null)
            {
                throw new QueryHandlerException("column type is null for: " + jpaFieldName);
            }
            f = (Field) col.getJavaMember();
        }

        if (f != null && f.getType() != null)
        {
            return HBaseUtils.getBytes(value, f.getType());
        }
        else
        {
            log.error("Error while handling data type for:" + jpaFieldName);
            throw new QueryHandlerException("field type is null for:" + jpaFieldName);
        }
    }
}
