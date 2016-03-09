/*******************************************************************************
 *  * Copyright 2016 Impetus Infotech.
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
package com.impetus.client.kudu.query;

import java.util.List;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.AbstractEntityReader;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;

/**
 * The Class KuduDBEntityReader.
 * 
 * @author karthikp.manchala
 */
public class KuduDBEntityReader extends AbstractEntityReader implements EntityReader
{

    /**
     * Instantiates a new kudu db entity reader.
     * 
     * @param kunderaMetadata
     *            the kundera metadata
     */
    public KuduDBEntityReader(KunderaMetadata kunderaMetadata)
    {
        super(kunderaMetadata);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.persistence.AbstractEntityReader#findById(java.lang.
     * Object, com.impetus.kundera.metadata.model.EntityMetadata,
     * com.impetus.kundera.client.Client)
     */
    @Override
    public EnhanceEntity findById(Object primaryKey, EntityMetadata m, Client client)
    {
        return super.findById(primaryKey, m, client);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.persistence.EntityReader#populateRelation(com.impetus
     * .kundera.metadata.model.EntityMetadata,
     * com.impetus.kundera.client.Client, int)
     */
    @Override
    public List<EnhanceEntity> populateRelation(EntityMetadata m, Client client, int maxResults)
    {
        // TODO Auto-generated method stub
        return null;
    }

}
