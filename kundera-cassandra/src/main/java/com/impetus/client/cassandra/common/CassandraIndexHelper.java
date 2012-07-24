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
package com.impetus.client.cassandra.common;

import org.apache.cassandra.thrift.Column;

import com.impetus.client.cassandra.config.CassandraPropertyReader;
import com.impetus.kundera.Constants;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorFactory;

/**
 * Provides functionalities related to indexing in cassandra
 * 
 * @author amresh.singh
 */
public class CassandraIndexHelper
{

    /**
     * Generates Secondary index name for a given column on a table
     * 
     * @param tableName
     * @param column
     * @return
     * @throws PropertyAccessException
     */
    public static String getSecondaryIndexName(String tableName, Column column) throws PropertyAccessException
    {
        String indexName = tableName + "_" + PropertyAccessorFactory.STRING.fromBytes(String.class, column.getName())
                + "_idx";
        return indexName;
    }

    /**
     * Generates inverted index table name for a given table
     * 
     * @param tableName
     * @return
     */
    public static String getInvertedIndexTableName(String tableName)
    {
        return tableName + Constants.INDEX_TABLE_SUFFIX;
    }

    /**
     * Checks whether Inverted indexing is applicable for a given entity whose
     * metadata is passed as parameter
     * 
     * @param m
     * @return
     */
    public static boolean isInvertedIndexingApplicable(EntityMetadata m)
    {
        boolean invertedIndexingApplicable = MetadataUtils.useSecondryIndex(m.getPersistenceUnit())
                && CassandraPropertyReader.csmd.isInvertedIndexingEnabled()
                && m.getType().isSuperColumnFamilyMetadata() && !m.isCounterColumnType();

        return invertedIndexingApplicable;
    }

}
