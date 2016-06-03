/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.kundera.configure.schema;

import java.util.List;


/**
 * @author amresh.singh
 *
 */
public class CollectionColumnInfo 
{
    /** The collection column name variable .*/
    private String collectionColumnName;  
    
    /**
     * Type of collection column among:
     * 1. java.util.List
     * 2. java.util.Set
     * 3. java.util.Map 
     */
    private Class<?> type;
    
    /**
     * Generic classes of data held in collection
     * would hold one element for Set and List, two for Map
     */
    private List<Class<?>> genericClasses;

    /**
     * @return the collectionColumnName
     */
    public String getCollectionColumnName()
    {
        return collectionColumnName;
    }

    /**
     * @param collectionColumnName the collectionColumnName to set
     */
    public void setCollectionColumnName(String collectionColumnName)
    {
        this.collectionColumnName = collectionColumnName;
    }

    /**
     * @return the type
     */
    public Class<?> getType()
    {
        return type;
    }

    /**
     * @param type the type to set
     */
    public void setType(Class<?> type)
    {
        this.type = type;
    }

    /**
     * @return the genericClasses
     */
    public List<Class<?>> getGenericClasses()
    {
        return genericClasses;
    }

    /**
     * @param genericClasses the genericClasses to set
     */
    public void setGenericClasses(List<Class<?>> genericClasses)
    {
        this.genericClasses = genericClasses;
    }   
    
    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj){
    	
    	//if object's class and column name matches then return true;
        return obj != null && obj instanceof CollectionColumnInfo && ((CollectionColumnInfo) obj).collectionColumnName != null ? this.collectionColumnName != null
                && this.collectionColumnName.equals(((CollectionColumnInfo) obj).collectionColumnName)
                : false;
    }

}
