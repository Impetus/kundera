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
package com.impetus.client.hbase;

import java.util.List;

import javax.persistence.PersistenceException;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.AbstractEntityReader;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.proxy.EnhancedEntity;

/**
 * The Class HBaseEntityReader.
 * 
 * @author vivek.mishra
 */
public class HBaseEntityReader extends AbstractEntityReader implements EntityReader
{

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.persistence.EntityReader#populateRelation(com.impetus
     * .kundera.metadata.model.EntityMetadata, java.util.List, boolean,
     * com.impetus.kundera.client.Client)
     */
    @Override
    public List<EnhanceEntity> populateRelation(EntityMetadata m, List<String> relationNames, boolean isParent,
            Client client)
    {
        throw new UnsupportedOperationException("Method is not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.persistence.EntityReader#findById(java.lang.String,
     * com.impetus.kundera.metadata.model.EntityMetadata, java.util.List,
     * com.impetus.kundera.client.Client)
     */
    @Override
    public EnhanceEntity findById(String primaryKey, EntityMetadata m, List<String> relationNames, Client client)
    {
        try
        {
            Object o = client.find(m.getEntityClazz(), primaryKey, relationNames);
            return (EnhanceEntity)o;
        }
        catch (Exception e)
        {
            throw new PersistenceException("Error while reading findById for Hbase" + e.getMessage());
        }

    }

}