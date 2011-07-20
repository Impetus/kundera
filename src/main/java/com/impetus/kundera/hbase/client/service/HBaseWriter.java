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
package com.impetus.kundera.hbase.client.service;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.impetus.kundera.hbase.client.Writer;
import com.impetus.kundera.metadata.MetadataManager;
import com.impetus.kundera.metadata.EntityMetadata.Column;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.proxy.EnhancedEntity;

/**
 * The Class HBaseWriter.
 *
 * @author impetus
 */
public class HBaseWriter implements Writer
{
    /** the log used by this class. */
    private static Log log = LogFactory.getLog(HBaseWriter.class);

    /*
     * (non-Javadoc)
     *
     * @see
     * com.impetus.kundera.hbase.client.Writer#writeColumns(org.apache.hadoop
     * .hbase.client.HTable, java.lang.String, java.lang.String, java.util.List,
     * com.impetus.kundera.proxy.EnhancedEntity)
     */
    @Override
    public void writeColumns(HTable htable, String columnFamily, String rowKey, List<Column> columns, Object columnFamilyObj)
            throws IOException
    {
        Put p = new Put(Bytes.toBytes(rowKey));       
        
        for (Column column : columns)
        {
            String qualifier = column.getName();
            try
            {
                
                p.add(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), PropertyAccessorHelper.get(columnFamilyObj,
                        column.getField()));
            }
            catch (PropertyAccessException e1)
            {
                throw new IOException(e1.getMessage());
            }
        }
        htable.put(p);
    }

}
