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
package com.impetus.client.cassandra.thrift;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.KeySlice;

import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.property.PropertyAccessor;
import com.impetus.kundera.property.PropertyAccessorFactory;

/**
 * Provides utility methods for extracting useful data from Thrift result
 * retrieved from database (usually in the form of {@link ColumnOrSuperColumn}
 * @author amresh.singh
 */
public class ThriftDataResultHelper
{
    public enum ColumnFamilyType {
        COLUMN,
        SUPER_COLUMN,
        COUNTER_COLUMN,
        COUNTER_SUPER_COLUMN
    }
    
    public static <T> List<T> fetchDataFromThriftResult(List<ColumnOrSuperColumn> coscList, ColumnFamilyType columnFamilyType)
    {
        List result = new ArrayList(coscList.size());
        for (ColumnOrSuperColumn cosc : coscList)
        {
            switch (columnFamilyType)
            {
            case COLUMN:
                result.add(cosc.column);
                break;

            case SUPER_COLUMN:
                result.add(cosc.super_column);
                break;
                
            case COUNTER_COLUMN:
                result.add(cosc.counter_column);
                break;
                
            case COUNTER_SUPER_COLUMN:
                result.add(cosc.counter_super_column);
                break;
            }
        }
        return result;
    }
    
    
    public static <T> List<T> fetchDataFromThriftResult(Map<ByteBuffer, List<ColumnOrSuperColumn>> coscResultMap, ColumnFamilyType columnFamilyType) {
        
        List<ColumnOrSuperColumn> coscList = new ArrayList<ColumnOrSuperColumn>();
        
        for(List<ColumnOrSuperColumn> list : coscResultMap.values()) {
            coscList.addAll(list);
        }
        
        return fetchDataFromThriftResult(coscList, columnFamilyType);        
    } 
    
    public static List<Object> getRowKeys(List<KeySlice> keySlices, EntityMetadata metadata) {
        PropertyAccessor<?> accessor = PropertyAccessorFactory.getPropertyAccessor(metadata.getIdColumn().getField());
        List<Object> rowKeys = new ArrayList<Object>();
        for (KeySlice keySlice : keySlices)
        {
            byte[] key = keySlice.getKey();
            Object rowKey = accessor.fromBytes(metadata.getIdColumn().getField().getType(), key);
            rowKeys.add(rowKey);
        }
        return rowKeys;
    }
    

}
