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
package com.impetus.kundera.utils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.persistence.ElementCollection;
import javax.persistence.Embedded;

import org.cloner.Cloner;

import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.Column;
import com.impetus.kundera.metadata.model.EmbeddedColumn;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * Provides utility methods for operation on objects
 * 
 * @author amresh.singh
 */
public class ObjectUtils
{

    public static final Object deepCopy(Object objectToCopy)
    {
        Object destObject = Cloner.deepClone(objectToCopy, objectToCopy.getClass());
        return destObject;

    }

    public static final Object deepCopyUsingMetadata(Object source)
    {

        Class<?> sourceObjectClass = source.getClass();
        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(sourceObjectClass);

        Object target = null;
        try
        {
            target = sourceObjectClass.newInstance();

            // Copy ID field
            Object id = metadata.getReadIdentifierMethod().invoke(source);
            
            metadata.getWriteIdentifierMethod().invoke(target, id);

            // Copy Columns (in a table that doesn't have any embedded objects
            for (Column column : metadata.getColumnsAsList())
            {
                Field columnField = column.getField();
                PropertyAccessorHelper.set(target, columnField, PropertyAccessorHelper.getObject(source, columnField));
            }

            // Copy Embedded Columns, Element Collections are columns (in a table that only has embedded objects)
            for (EmbeddedColumn embeddedColumn : metadata.getEmbeddedColumnsAsList())
            {
                Field embeddedColumnField = embeddedColumn.getField();
                
                Annotation embeddedAnnotation = embeddedColumnField.getAnnotation(Embedded.class);
                Annotation ecAnnotation = embeddedColumnField.getAnnotation(ElementCollection.class);
                Annotation columnAnnotation = embeddedColumnField.getAnnotation(javax.persistence.Column.class);
                
                Object sourceEmbeddedObj = PropertyAccessorHelper.getObject(source, embeddedColumnField);
                
                
                if(embeddedAnnotation != null) {                    
                    //Copy embedded objects
                    Class<?> embeddedColumnClass = embeddedColumnField.getType();                    
                    Object targetEmbeddedObj = embeddedColumnClass.newInstance();                    

                    for (Column column : embeddedColumn.getColumns())
                    {
                        PropertyAccessorHelper.set(targetEmbeddedObj, column.getField(),
                                PropertyAccessorHelper.getObject(sourceEmbeddedObj, column.getField()));
                    }
                    
                    PropertyAccessorHelper.set(target, embeddedColumnField, targetEmbeddedObj);
                } else if(ecAnnotation != null){
                    //Copy element collections
                    if(sourceEmbeddedObj instanceof Collection) {
                        Class<?> ecDeclaredClass = embeddedColumnField.getType();
                        Class<?> actualEcObjectClass = sourceEmbeddedObj.getClass();                        
                        Class<?> genericClass = PropertyAccessorHelper.getGenericClass(embeddedColumnField);
                        
                        Object targetCollectionObject = actualEcObjectClass.newInstance();
                        
                        for(Object sourceEcObj : (Collection)sourceEmbeddedObj) {
                            Object targetEcObj = genericClass.newInstance();
                            for(Field f : genericClass.getDeclaredFields()) {
                               PropertyAccessorHelper.set(targetEcObj, f, PropertyAccessorHelper.getObject(sourceEcObj, f));                                
                                
                            }                          
                            
                            if(List.class.isAssignableFrom(ecDeclaredClass)) {
                                Method m = actualEcObjectClass.getMethod("add", Object.class);                                
                                m.invoke(targetCollectionObject, targetEcObj);
                                
                            } else if(Set.class.isAssignableFrom(ecDeclaredClass)) {
                                Method m = actualEcObjectClass.getMethod("add", Object.class);                                
                                m.invoke(targetCollectionObject, targetEcObj);
                            }
                            
                        }
                        PropertyAccessorHelper.set(target, embeddedColumnField, targetCollectionObject);
                        
                        
                    }                 
                    
                    
                } else if(columnAnnotation != null){
                    //Copy columns
                    PropertyAccessorHelper.set(target, embeddedColumnField, sourceEmbeddedObj);
                }
            }


            //Copy Relationships recursively
            for(Relation relation : metadata.getRelations()) {
                Field relationField = relation.getProperty();
                Object sourceRelationObject = PropertyAccessorHelper.getObject(source, relationField);
                
                Object targetRelationObject = null;
                
                Class<?> relationObjectClass = relation.getPropertyType();
                Class<?> actualRelationObjectClass = sourceRelationObject.getClass();
                Class<?> genericClass = relation.getTargetEntity();
                
                if(!Collection.class.isAssignableFrom(relationObjectClass)) {
                    targetRelationObject = deepCopy(sourceRelationObject);
                } else {
                    targetRelationObject = actualRelationObjectClass.newInstance();
                    Method m = actualRelationObjectClass.getMethod("add", Object.class);                    
                    
                    for(Object obj : (Collection)sourceRelationObject) {
                        Object copyTargetRelObj = deepCopy(obj);
                        m.invoke(targetRelationObject, copyTargetRelObj);
                    }
                }                    
                
                PropertyAccessorHelper.set(target, relationField, targetRelationObject);
            }

        }
        catch (InstantiationException e)
        {
            e.printStackTrace();
            return null;
        }
        catch (IllegalAccessException e)
        {
            e.printStackTrace();
            return null;
        }

        catch (InvocationTargetException e)
        {
            e.printStackTrace();
            return null;
        }
        
        catch (NoSuchMethodException e)
        {
            e.printStackTrace();
            return null;
        }

        return target;
    }
    
    
    /**
     * Gets the field instance.
     * 
     * @param chids
     *            the chids
     * @param f
     *            the f
     * @return the field instance
     */
    public static Object getFieldInstance(List chids, Field f)
    {

        if (Set.class.isAssignableFrom(f.getType()))
        {
            Set col = new HashSet(chids);
            return col;
        }
        return chids;
    }

}
