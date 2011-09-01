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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.PersistenceException;

import com.impetus.kundera.Constants;
import com.impetus.kundera.metadata.EntityMetadata.Column;
import com.impetus.kundera.metadata.EntityMetadata.SuperColumn;
import com.impetus.kundera.property.PropertyAccessorHelper;

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
            for (EntityMetadata.Column column : entry.getValue().getColumns())
            {                
                columnNameToFieldMap.put(column.getName(), column.getField());
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
    
    /**
     * @param m
     * @param columnNameToFieldMap
     * @param superColumnNameToFieldMap
     */
    public static Map<String, Field> createSuperColumnsFieldMap(EntityMetadata m)
    {
        Map<String, Field> superColumnNameToFieldMap = new HashMap<String, Field>();
        for (Map.Entry<String, EntityMetadata.SuperColumn> entry : m.getSuperColumnsMap().entrySet())
        {
            EntityMetadata.SuperColumn scMetadata = entry.getValue();
            superColumnNameToFieldMap.put(scMetadata.getName(), scMetadata.getField());
            
        }
        return superColumnNameToFieldMap;

    }
    
    public static Collection getEmbeddedCollectionInstance(Field embeddedCollectionField) {
        Collection embeddedCollection = null;
        Class embeddedCollectionFieldClass = embeddedCollectionField.getType();

        if (embeddedCollection == null || embeddedCollection.isEmpty())
        {
            if (embeddedCollectionFieldClass.equals(List.class))
            {
                embeddedCollection = new ArrayList<Object>();
            }
            else if (embeddedCollectionFieldClass.equals(Set.class))
            {
                embeddedCollection = new HashSet<Object>();
            }
            else
            {
                throw new PersistenceException("Field " + embeddedCollectionField.getName() + " must be either instance of List or Set");
            }
        }
        return embeddedCollection;
    }
    
    public static Object getEmbeddedGenericObjectInstance(Field embeddedCollectionField) {
        Class<?> embeddedClass = PropertyAccessorHelper.getGenericClass(embeddedCollectionField);
        Object embeddedObject = null;
        // must have a default no-argument constructor
        try
        {
            embeddedClass.getConstructor();
            embeddedObject = embeddedClass.newInstance();
        }
        catch (NoSuchMethodException nsme)
        {
            throw new PersistenceException(embeddedClass.getName()
                    + " is @Embeddable and must have a default no-argument constructor.");
        } 
        catch(InstantiationException e)
        {
            throw new PersistenceException(embeddedClass.getName() + " could not be instantiated");
        }
        
        catch(IllegalAccessException e)
        {
            throw new PersistenceException(embeddedClass.getName() + " could not be accessed");
        }
        return embeddedObject;        
    }
    
    public static String getEmbeddedCollectionPrefix(String embeddedCollectionName) {
        return embeddedCollectionName.substring(0, embeddedCollectionName.indexOf(Constants.SUPER_COLUMN_NAME_DELIMITER));
    }
    
    public static String getEmbeddedCollectionPostfix(String embeddedCollectionName) {
        return embeddedCollectionName.substring(embeddedCollectionName.indexOf(Constants.SUPER_COLUMN_NAME_DELIMITER) + 1, embeddedCollectionName.length());
    }  
    
    /**
     * Creates a string representation of a set of foreign keys by combining
     * them together separated by "~" character.
     *
     * Note: Assumption is that @Id will never contain "~" character. Checks for
     * this are not added yet.
     *
     * @param foreignKeys
     *            the foreign keys
     * @return the string
     */
    public static String serializeKeys(Set<String> foreignKeys)
    {
        if (null == foreignKeys || foreignKeys.isEmpty())
        {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        for (String key : foreignKeys)
        {
            if (sb.length() > 0)
            {
                sb.append(Constants.SEPARATOR);
            }
            sb.append(key);
        }
        return sb.toString();
    }

    /**
     * Splits foreign keys into Set.
     *
     * @param foreignKeys
     *            the foreign keys
     * @return the set
     */
    public static Set<String> deserializeKeys(String foreignKeys)
    {
        Set<String> keys = new HashSet<String>();

        if (null == foreignKeys || foreignKeys.isEmpty())
        {
            return keys;
        }

        String array[] = foreignKeys.split(Constants.SEPARATOR);
        for (String element : array)
        {
            keys.add(element);
        }
        return keys;
    }  

}
