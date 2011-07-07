/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
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
package com.impetus.kundera.hbase.client;

import java.io.IOException;
import java.util.List;

import com.impetus.kundera.metadata.EntityMetadata.Column;
import com.impetus.kundera.proxy.EnhancedEntity;

/**
 * HBase client for interaction with HBase.
 *
 * @author impetus
 */
public interface Client
{

    /**
     *
     * @param columnFamily
     * @param rowKey
     * @param columns
     */
    void write(String tableName, String columnFamily, String rowKey, List<Column> columns, EnhancedEntity e)
            throws IOException;

    /**
     *
     * @param columnFamily
     * @param rowKey
     * @return
     */
    HBaseData read(String tableName, String columnFamily, String rowKey, String... columnNames) throws IOException;

}
