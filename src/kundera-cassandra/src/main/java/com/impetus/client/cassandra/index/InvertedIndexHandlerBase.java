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
package com.impetus.client.cassandra.index;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.thrift.SuperColumn;
import org.scale7.cassandra.pelops.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.Constants;
import com.impetus.kundera.db.SearchResult;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.DefaultSingularAttribute;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.query.QueryHandlerException;

/**
 * Base class for
 * 
 * @author amresh.singh
 */
public abstract class InvertedIndexHandlerBase
{
    /** log for this class. */
    private static Logger log = LoggerFactory.getLogger(InvertedIndexHandlerBase.class);
    
    protected boolean useSecondryIndex;

    public List<SearchResult> search(EntityMetadata m, String persistenceUnit, ConsistencyLevel consistencyLevel,
            Map<Boolean, List<IndexClause>> indexClauseMap)
    {
        String columnFamilyName = m.getTableName() + Constants.INDEX_TABLE_SUFFIX;

        List<SearchResult> searchResults = new ArrayList<SearchResult>();

        boolean isRowKeyQuery = indexClauseMap.keySet().iterator().next();

        for (IndexClause o : indexClauseMap.get(isRowKeyQuery))
        {
            for (IndexExpression expression : o.getExpressions())
            {
                searchAndAddToResults(m, persistenceUnit, consistencyLevel, columnFamilyName, searchResults,
                        expression, isRowKeyQuery);
            }

        }
        return searchResults;
    }

    /**
     * Searches into inverted index based on <code>expression</code> and adds
     * search result to <code>searchResults</code>
     */
    private void searchAndAddToResults(EntityMetadata m, String persistenceUnit, ConsistencyLevel consistencyLevel,
            String columnFamilyName, List<SearchResult> searchResults, IndexExpression expression,
            boolean isRowKeyQuery)
    {
        SearchResult searchResult = new SearchResult();

        String rowKey = Bytes.toUTF8(expression.getColumn_name());
        byte[] superColumnName = expression.getValue();
        String superColumnNameStr = Bytes.toUTF8(expression.getValue());
        Object pk = PropertyAccessorHelper.getObject(m.getIdAttribute().getJavaType(), superColumnName);
        IndexOperator condition = expression.getOp();

        if (log.isInfoEnabled())
        {
            log.info("RowKey: {} ; Super column Name: {} on condition.",rowKey,superColumnNameStr, condition);
        }

        // TODO: Second check unnecessary but unavoidable as filter clause
        // property is incorrectly passed as column name

        // Search based on Primary key
        if (isRowKeyQuery
                && (rowKey.equals(m.getIdAttribute().getName()) || rowKey.equals(((DefaultSingularAttribute) m
                        .getIdAttribute()).getJPAColumnName())))
        {
            if (searchResults.isEmpty())
            {
                searchResult.setPrimaryKey(pk);
                searchResults.add(searchResult);
            }
            else
            {
                SearchResult existing = searchResults.get(0);
                if (existing.getPrimaryKey() != null && existing.getPrimaryKey().equals(superColumnNameStr))
                {
                    searchResults.add(searchResult);
                }
                else
                {
                    searchResults.remove(0);
                }
            }
        }
        else
        {
            // Search results in the form of thrift super columns
            List<SuperColumn> thriftSuperColumns = new ArrayList<SuperColumn>();

            switch (condition)
            {
            // EQUAL Operator
            case EQ:
                SuperColumn thriftSuperColumn = getSuperColumnForRow(consistencyLevel, columnFamilyName, rowKey,
                        superColumnName, persistenceUnit);

                if (thriftSuperColumn != null)
                    thriftSuperColumns.add(thriftSuperColumn);
                break;

            // LIKE operation not available
            /*
             * case LIKE: searchColumnsInRange(columnFamilyName,
             * consistencyLevel, persistenceUnit, rowKey, columnName,
             * thriftColumns, columnName.getBytes(), new byte[0]); break;
             */

            // Greater than operator
            case GT:
                searchSuperColumnsInRange(columnFamilyName, consistencyLevel, persistenceUnit, rowKey, superColumnName,
                        thriftSuperColumns, superColumnName, new byte[0]);
                break;
            // Less than Operator
            case LT:
                searchSuperColumnsInRange(columnFamilyName, consistencyLevel, persistenceUnit, rowKey, superColumnName,
                        thriftSuperColumns, new byte[0], superColumnName);
                break;
            // Greater than-equals to operator
            case GTE:
                searchSuperColumnsInRange(columnFamilyName, consistencyLevel, persistenceUnit, rowKey, superColumnName,
                        thriftSuperColumns, superColumnName, new byte[0]);
                break;
            // Less than equal to operator
            case LTE:
                searchSuperColumnsInRange(columnFamilyName, consistencyLevel, persistenceUnit, rowKey, superColumnName,
                        thriftSuperColumns, new byte[0], superColumnName);
                break;

            default:
                throw new QueryHandlerException(condition
                        + " comparison operator not supported currently for Cassandra Inverted Index.");

            }

            // Construct search results out of these thrift columns
            for (SuperColumn thriftSuperColumn : thriftSuperColumns)
            {

                for (Column column : thriftSuperColumn.getColumns())
                {
                    byte[] columnName = column.getName();
                    searchResult.setPrimaryKey(PropertyAccessorHelper.getObject(m.getIdAttribute().getJavaType(),
                            columnName));
                    byte[] columnValue = column.getValue();
                    String ecValue = Bytes.toUTF8(columnValue);

                    if (ecValue != null && !"".equals(ecValue.trim()))
                    {
                        searchResult.setEmbeddedColumnName(rowKey.substring(0,
                                rowKey.indexOf(Constants.INDEX_TABLE_ROW_KEY_DELIMITER)));
                        searchResult.addEmbeddedColumnValue(ecValue);
                    }
                }

                if (searchResults.isEmpty())
                {
                    searchResults.add(searchResult);
                }
                else
                {
                    SearchResult existing = searchResults.get(0);
                    if (existing.getPrimaryKey() != null
                            && existing.getPrimaryKey().equals(searchResult.getPrimaryKey()))
                    {
                        searchResults.add(searchResult);
                    }
                    else
                    {
                        searchResults.remove(0);
                    }
                }

            }
        }
    }

    public void delete(Object entity, EntityMetadata metadata, ConsistencyLevel consistencyLevel)
    {
        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                metadata.getPersistenceUnit());
        if (CassandraIndexHelper.isInvertedIndexingApplicable(metadata, useSecondryIndex))
        {

            String indexColumnFamily = CassandraIndexHelper.getInvertedIndexTableName(metadata.getTableName());
            Map<String, EmbeddableType> embeddables = metaModel.getEmbeddables(metadata.getEntityClazz());
            // for (EmbeddedColumn embeddedColumn :
            // metadata.getEmbeddedColumnsAsList())
            EntityType entityType = metaModel.entity(metadata.getEntityClazz());

            byte[] columnName = PropertyAccessorHelper.get(entity, (Field) metadata.getIdAttribute().getJavaMember());

            for (String fieldName : embeddables.keySet())
            {
                EmbeddableType embeddedColumn = embeddables.get(fieldName);
                Attribute embeddedAttribute = entityType.getAttribute(fieldName);
                // Object embeddedObject =
                // PropertyAccessorHelper.getObject(entity,
                // embeddedColumn.getField());
                Object embeddedObject = PropertyAccessorHelper.getObject(entity,
                        (Field) embeddedAttribute.getJavaMember());

                if (embeddedObject != null)
                {
                    if (embeddedObject instanceof Collection)
                    {
                        for (Object obj : (Collection) embeddedObject)
                        {
                            Iterator<Attribute> iter = embeddedColumn.getAttributes().iterator();
                            while (iter.hasNext())
                            {
                                Attribute attrib = iter.next();
                                String rowKey = embeddedAttribute.getName() + Constants.INDEX_TABLE_ROW_KEY_DELIMITER
                                        + attrib.getName();
                                byte[] superColumnName = PropertyAccessorHelper
                                        .get(obj, (Field) attrib.getJavaMember());
                                if (superColumnName != null)
                                {
                                    deleteColumn(indexColumnFamily, rowKey, superColumnName,
                                            metadata.getPersistenceUnit(), consistencyLevel, columnName);
                                }
                            }
                        }
                    }
                    else
                    {

                        Iterator<Attribute> iter = embeddedColumn.getAttributes().iterator();
                        while (iter.hasNext())
                        {
                            Attribute attrib = iter.next();
                            String rowKey = embeddedAttribute.getName() + Constants.INDEX_TABLE_ROW_KEY_DELIMITER
                                    + attrib.getName();
                            byte[] superColumnName = PropertyAccessorHelper.get(embeddedObject,
                                    (Field) attrib.getJavaMember());
                            if (superColumnName != null)
                            {
                                deleteColumn(indexColumnFamily, rowKey, superColumnName, metadata.getPersistenceUnit(),
                                        consistencyLevel, columnName);
                            }

                        }
                    }
                }
            }
        }
    }

    /**
     * @param indexColumnFamily
     * @param rowKey
     * @param superColumnName
     * @param columnName
     *            TODO
     */
    protected abstract void deleteColumn(String indexColumnFamily, String rowKey, byte[] superColumnName,
            String persistenceUnit, ConsistencyLevel consistencyLevel, byte[] columnName);

    /**
     * @param consistencyLevel
     * @param columnFamilyName
     * @param rowKey
     * @param superColumnName
     * @return
     */
    protected abstract SuperColumn getSuperColumnForRow(ConsistencyLevel consistencyLevel, String columnFamilyName,
            String rowKey, byte[] superColumnName, String persistenceUnit);

    protected abstract void searchSuperColumnsInRange(String columnFamilyName, ConsistencyLevel consistencyLevel,
            String persistenceUnit, String rowKey, byte[] searchSuperColumnName, List<SuperColumn> thriftSuperColumns,
            byte[] start, byte[] finish);

}
