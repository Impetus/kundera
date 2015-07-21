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
package com.impetus.spark.datahandler;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.SingularAttribute;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.utils.KunderaCoreUtils;

/**
 * The Class SparkDataHandler.
 * 
 * @author: pragalbh.garg
 */
public class SparkDataHandler
{

    /** The kundera metadata. */
    private KunderaMetadata kunderaMetadata;

    /**
     * Instantiates a new spark data handler.
     * 
     * @param kunderaMetadata
     *            the kundera metadata
     */
    public SparkDataHandler(KunderaMetadata kunderaMetadata)
    {
        this.kunderaMetadata = kunderaMetadata;
    }

    /**
     * Load data and populate results.
     * 
     * @param dataFrame
     *            the data frame
     * @param m
     *            the m
     * @param kunderaQuery
     *            the kundera query
     * @return the list
     */
    public List<?> loadDataAndPopulateResults(DataFrame dataFrame, EntityMetadata m, KunderaQuery kunderaQuery)
    {
        if (kunderaQuery != null && kunderaQuery.isAggregated())
        {
            return dataFrame.collectAsList();
        }
        // TODO: handle the case of specific field selection
        else
        {
            return populateEntityObjectsList(dataFrame, m);
        }
    }

    /**
     * Populate entity objects list.
     * 
     * @param dataFrame
     *            the data frame
     * @param m
     *            the m
     * @return the list
     */
    private List<?> populateEntityObjectsList(DataFrame dataFrame, EntityMetadata m)
    {
        List results = new ArrayList();
        String[] columns = dataFrame.columns();
        Map<String, Integer> map = createMapOfColumnIndex(columns);
        for (Row row : dataFrame.collectAsList())
        {
            Object entity = populateEntityFromDataFrame(m, map, row);
            results.add(entity);
        }
        return results;
    }

    /**
     * Populate entity from data frame.
     * 
     * @param m
     *            the m
     * @param columnIndexMap
     *            the column index map
     * @param row
     *            the row
     * @return the object
     */
    private Object populateEntityFromDataFrame(EntityMetadata m, Map<String, Integer> columnIndexMap, Row row)
    {
        try
        {
            // create entity instance
            Object entity = KunderaCoreUtils.createNewInstance(m.getEntityClazz());
            // handle relations
            Map<String, Object> relations = new HashMap<String, Object>();
            if (entity.getClass().isAssignableFrom(EnhanceEntity.class))
            {
                relations = ((EnhanceEntity) entity).getRelations();
                entity = ((EnhanceEntity) entity).getEntity();
            }
            // get a Set of attributes
            MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                    m.getPersistenceUnit());
            EntityType entityType = metaModel.entity(m.getEntityClazz());
            Set<Attribute> attributes = ((AbstractManagedType) entityType).getAttributes();
            // iterate over attributes and find its value
            for (Attribute attribute : attributes)
            {
                String columnName = getColumnName(attribute);
                Object columnValue = row.get(columnIndexMap.get(columnName));
                if (columnValue != null)
                {
                    Object value = PropertyAccessorHelper.fromSourceToTargetClass(attribute.getJavaType(),
                            columnValue.getClass(), columnValue);

                    PropertyAccessorHelper.set(entity, (Field) attribute.getJavaMember(), value);
                }
            }
            // find the value of @Id field for EnhanceEntity
            SingularAttribute attrib = m.getIdAttribute();
            Object rowKey = PropertyAccessorHelper.getObject(entity, (Field) attrib.getJavaMember());
            if (!relations.isEmpty())
            {
                return new EnhanceEntity(entity, rowKey, relations);
            }
            return entity;
        }
        catch (PropertyAccessException e1)
        {
            throw new RuntimeException(e1);
        }
    }

    /**
     * Gets the column name.
     * 
     * @param attribute
     *            the attribute
     * @return the column name
     */
    public String getColumnName(Attribute attribute)
    {
        return attribute.getName();
    }

    /**
     * Creates the map of column index.
     * 
     * @param columns
     *            the columns
     * @return the map
     */
    private Map<String, Integer> createMapOfColumnIndex(String[] columns)
    {
        int i = 0;
        Map<String, Integer> map = new HashMap<String, Integer>();
        for (String column : columns)
        {
            map.put(column, i++);
        }
        return map;
    }

}
