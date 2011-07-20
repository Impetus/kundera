/*
 * Copyright 2011 Impetus Infotech.
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
package com.impetus.kundera.metadata;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import com.impetus.kundera.metadata.EntityMetadata.Column;
import com.impetus.kundera.metadata.EntityMetadata.SuperColumn;

/**
 * Utility class for entity metadata related funcntionality
 * 
 * @author amresh.singh
 */
public class MetadataUtils
{
    /**
     * @param m
     * @param columnNameToFieldMap
     * @param superColumnNameToFieldMap
     */
    public static void populateColumnAndSuperColumnMaps(EntityMetadata m, Map<String, Field> columnNameToFieldMap,
            Map<String, Field> superColumnNameToFieldMap)
    {
        for (Map.Entry<String, EntityMetadata.SuperColumn> entry : m.getSuperColumnsMap().entrySet())
        {
            EntityMetadata.SuperColumn scMetadata = entry.getValue();
            superColumnNameToFieldMap.put(scMetadata.getName(), scMetadata.getField());
            for (EntityMetadata.Column cMetadata : entry.getValue().getColumns())
            {                
                columnNameToFieldMap.put(cMetadata.getName(), cMetadata.getField());
            }
        }
    }

    /**
     * @param m
     * @param columnNameToFieldMap
     * @param superColumnNameToFieldMap
     */
    public static Map<String, Field> createColumnsFieldMap(EntityMetadata m, SuperColumn superColumn)
    {
        Map<String, Field> columnNameToFieldMap = new HashMap<String, Field>();
        for (EntityMetadata.Column column : superColumn.getColumns())
        {
            columnNameToFieldMap.put(column.getName(), column.getField());
        }
        return columnNameToFieldMap;

    }

}
