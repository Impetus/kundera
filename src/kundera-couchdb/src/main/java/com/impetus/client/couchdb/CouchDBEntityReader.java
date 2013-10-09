package com.impetus.client.couchdb;

import java.util.List;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.AbstractEntityReader;
import com.impetus.kundera.persistence.EntityReader;

public class CouchDBEntityReader extends AbstractEntityReader implements EntityReader
{


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
