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
package com.impetus.client.couchdb;

import java.util.List;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.AbstractEntityReader;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.query.KunderaQuery;

/**
 * CouchDBEntity Reader.
 * 
 * @author Kuldeep Mishra
 * 
 */
public class CouchDBEntityReader extends AbstractEntityReader implements EntityReader
{

    /**
     * Instantiates a new couch db entity reader.
     * 
     * @param kunderaMetadata
     *            the kundera metadata
     */
    public CouchDBEntityReader(final KunderaMetadata kunderaMetadata)
    {
        super(kunderaMetadata);
    }

    /**
     * Instantiates a new couch db entity reader.
     * 
     * @param kunderaQuery
     *            the kundera query
     * @param kunderaMetadata
     *            the kundera metadata
     */
    public CouchDBEntityReader(KunderaQuery kunderaQuery, final KunderaMetadata kunderaMetadata)
    {
        super(kunderaMetadata);
        this.kunderaQuery = kunderaQuery;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.persistence.EntityReader#populateRelation(com.impetus
     * .kundera.metadata.model.EntityMetadata,
     * com.impetus.kundera.client.Client)
     */
    @Override
    public List<EnhanceEntity> populateRelation(EntityMetadata m, Client client, int maxResults)
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Method supported not required for Redis");
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
    public EnhanceEntity findById(Object primaryKey, EntityMetadata m, Client client)
    {
        return super.findById(primaryKey, m, client);
    }

}
