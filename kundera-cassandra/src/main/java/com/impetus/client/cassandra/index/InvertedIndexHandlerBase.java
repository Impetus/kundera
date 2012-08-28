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
import java.util.Queue;

import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.scale7.cassandra.pelops.Bytes;

import com.impetus.kundera.Constants;
import com.impetus.kundera.db.SearchResult;
import com.impetus.kundera.metadata.model.EmbeddedColumn;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.DefaultSingularAttribute;
import com.impetus.kundera.property.PropertyAccessor;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.query.KunderaQuery.FilterClause;
import com.impetus.kundera.query.QueryHandlerException;

/**
 * Base class for
 * 
 * @author amresh.singh
 */
public abstract class InvertedIndexHandlerBase
{
    /** log for this class. */
    private static Log log = LogFactory.getLog(InvertedIndexHandlerBase.class);

    public List<SearchResult> search(EntityMetadata m, Queue<FilterClause> filterClauseQueue, String persistenceUnit,
            ConsistencyLevel consistencyLevel)
    {
        String columnFamilyName = m.getTableName() + Constants.INDEX_TABLE_SUFFIX;

        List<SearchResult> searchResults = new ArrayList<SearchResult>();

        for (FilterClause o : filterClauseQueue)
        {
            SearchResult searchResult = new SearchResult();

            FilterClause clause = ((FilterClause) o);
            String rowKey = clause.getProperty();
            String columnName = clause.getValue().toString();

            String condition = clause.getCondition();
            log.debug("rowKey:" + rowKey + ";columnName:" + columnName + ";condition:" + condition);

            // TODO: Second check unnecessary but unavoidable as filter clause
            // property is incorrectly passed as column name

            // Search based on Primary key
//            if (rowKey.equals(m.getIdAttribute()().getField().getName()) || rowKey.equals(m.getIdColumn().getName()))
            if (rowKey.equals(m.getIdAttribute().getName()) || rowKey.equals(((DefaultSingularAttribute)m.getIdAttribute()).getJPAColumnName()))
          {

                searchResult.setPrimaryKey(columnName);

            }
            else
            {
                // Search results in the form of thrift columns
                List<Column> thriftColumns = new ArrayList<Column>();

                // EQUAL Operator
                if (condition.equals("="))
                {
                    Column thriftColumn = getColumnForRow(consistencyLevel, columnFamilyName, rowKey, columnName,
                            persistenceUnit);
                    thriftColumns.add(thriftColumn);
                }

                // LIKE operation
                else if (condition.equalsIgnoreCase("LIKE"))
                {

                    searchColumnsInRange(columnFamilyName, consistencyLevel, persistenceUnit, rowKey, columnName,
                            thriftColumns, columnName.getBytes(), new byte[0]);
                }

                // Greater than operator
                else if (condition.equals(">"))
                {
                    searchColumnsInRange(columnFamilyName, consistencyLevel, persistenceUnit, rowKey, columnName,
                            thriftColumns, columnName.getBytes(), new byte[0]);
                }

                // Less than Operator
                else if (condition.equals("<"))
                {
                    searchColumnsInRange(columnFamilyName, consistencyLevel, persistenceUnit, rowKey, columnName,
                            thriftColumns, new byte[0], columnName.getBytes());
                }

                // Greater than-equals to operator
                else if (condition.equals(">="))
                {
                    searchColumnsInRange(columnFamilyName, consistencyLevel, persistenceUnit, rowKey, columnName,
                            thriftColumns, columnName.getBytes(), new byte[0]);
                }

                // Less than equal to operator
                else if (condition.equals("<="))
                {
                    searchColumnsInRange(columnFamilyName, consistencyLevel, persistenceUnit, rowKey, columnName,
                            thriftColumns, new byte[0], columnName.getBytes());
                }
                else
                {
                    throw new QueryHandlerException(condition
                            + " comparison operator not supported currently for Cassandra Inverted Index");
                }

                // Construct search results out of these thrift columns
                for (Column thriftColumn : thriftColumns)
                {
                    byte[] columnValue = thriftColumn.getValue();
                    String columnValueStr = Bytes.toUTF8(columnValue);

                    PropertyAccessor<?> accessor = PropertyAccessorFactory.getPropertyAccessor((Field) m.getIdAttribute().getJavaMember());
                    Object value = null;

                    if (columnValueStr.indexOf(Constants.INDEX_TABLE_EC_DELIMITER) > 0)
                    {
                        String pk = columnValueStr.substring(0,
                                columnValueStr.indexOf(Constants.INDEX_TABLE_EC_DELIMITER));
                        String ecName = columnValueStr.substring(
                                columnValueStr.indexOf(Constants.INDEX_TABLE_EC_DELIMITER)
                                        + Constants.INDEX_TABLE_EC_DELIMITER.length(), columnValueStr.length());

                        searchResult.setPrimaryKey(pk);
                        searchResult.setEmbeddedColumnName(rowKey.substring(0,
                                rowKey.indexOf(Constants.INDEX_TABLE_ROW_KEY_DELIMITER)));
                        searchResult.addEmbeddedColumnValue(ecName);

                    }
                    else
                    {
                        value = accessor.fromBytes(m.getIdAttribute().getJavaType(), columnValue);
                        searchResult.setPrimaryKey(value);
                    }
                    searchResults.add(searchResult);
                }

            }

        }
        return searchResults;
    }

    public void delete(Object entity, EntityMetadata metadata, ConsistencyLevel consistencyLevel)
    {
        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(metadata.getPersistenceUnit());
        if (CassandraIndexHelper.isInvertedIndexingApplicable(metadata))
        {

            String indexColumnFamily = CassandraIndexHelper.getInvertedIndexTableName(metadata.getTableName());
            Map<String, EmbeddableType> embeddables = metaModel.getEmbeddables(metadata.getEntityClazz());
//            for (EmbeddedColumn embeddedColumn : metadata.getEmbeddedColumnsAsList())
            EntityType entityType = metaModel.entity(metadata.getEntityClazz());
            for(String fieldName : embeddables.keySet())
            {
                EmbeddableType embeddedColumn = embeddables.get(fieldName);
                Attribute embeddedAttribute = entityType.getAttribute(fieldName);
//                Object embeddedObject = PropertyAccessorHelper.getObject(entity, embeddedColumn.getField());
                Object embeddedObject = PropertyAccessorHelper.getObject(entity, (Field) embeddedAttribute.getJavaMember());
                
                if (embeddedObject != null)
                {
                    if (embeddedObject instanceof Collection)
                    {
                        for (Object obj : (Collection) embeddedObject)
                        {
                            Iterator<Attribute> iter = embeddedColumn.getAttributes().iterator();
                            while(iter.hasNext())
//                            for(Attribute column : embeddedColumn.getAttributes())
//                            for (com.impetus.kundera.metadata.model.Column column : embeddedColumn.getColumns())
                            {
                                Attribute attrib = iter.next();
                                String rowKey = embeddedAttribute.getName()
                                        + Constants.INDEX_TABLE_ROW_KEY_DELIMITER + attrib.getName();
                                byte[] columnName = PropertyAccessorHelper.get(obj, (Field) attrib.getJavaMember());
                                if (columnName != null)
                                {
                                    deleteColumn(indexColumnFamily, rowKey, columnName, metadata.getPersistenceUnit(),
                                            consistencyLevel);
                                }

                            }
                        }

                    }
                    else
                    {

                        Iterator<Attribute> iter = embeddedColumn.getAttributes().iterator();
                        while(iter.hasNext())
//                        for(Attribute column : embeddedColumn.getAttributes())
//                        for (com.impetus.kundera.metadata.model.Column column : embeddedColumn.getColumns())
                        {
                            Attribute attrib = iter.next();
                            String rowKey = embeddedAttribute.getName()
                                    + Constants.INDEX_TABLE_ROW_KEY_DELIMITER + attrib.getName();
                            byte[] columnName = PropertyAccessorHelper.get(embeddedObject, (Field) attrib.getJavaMember());
                            if (columnName != null)
                            {
                                deleteColumn(indexColumnFamily, rowKey, columnName, metadata.getPersistenceUnit(),
                                        consistencyLevel);
                            }

                        }
                        
//                        for (com.impetus.kundera.metadata.model.Column column : embeddedColumn.getColumns())
//                        {
//                            String rowKey = embeddedColumn.getField().getName()
//                                    + Constants.INDEX_TABLE_ROW_KEY_DELIMITER + column.getField().getName();
//                            byte[] columnName = PropertyAccessorHelper.get(embeddedObject, column.getField());
//                            if (columnName != null)
//                            {
//                                deleteColumn(indexColumnFamily, rowKey, columnName, metadata.getPersistenceUnit(),
//                                        consistencyLevel);
//                            }
//                        }
                    }
                }
            }
        }
    }

    /**
     * @param indexColumnFamily
     * @param rowKey
     * @param columnName
     */
    protected abstract void deleteColumn(String indexColumnFamily, String rowKey, byte[] columnName,
            String persistenceUnit, ConsistencyLevel consistencyLevel);

    /**
     * @param consistencyLevel
     * @param columnFamilyName
     * @param rowKey
     * @param columnName
     * @return
     */
    protected abstract Column getColumnForRow(ConsistencyLevel consistencyLevel, String columnFamilyName,
            String rowKey, String columnName, String persistenceUnit);

    protected abstract void searchColumnsInRange(String columnFamilyName, ConsistencyLevel consistencyLevel,
            String persistenceUnit, String rowKey, String searchString, List<Column> thriftColumns, byte[] start,
            byte[] finish);

}
