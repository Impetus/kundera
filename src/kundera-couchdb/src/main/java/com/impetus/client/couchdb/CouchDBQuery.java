package com.impetus.client.couchdb;

import java.util.Iterator;
import java.util.List;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.query.QueryImpl;

public class CouchDBQuery extends QueryImpl
{

    public CouchDBQuery(String query, PersistenceDelegator persistenceDelegator)
    {
        super(query, persistenceDelegator);
        // TODO Auto-generated constructor stub
    }



    @Override
    protected List populateEntities(EntityMetadata m, Client client)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected List recursivelyPopulateEntities(EntityMetadata m, Client client)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected EntityReader getReader()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected int onExecuteUpdate()
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void close()
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public Iterator iterate()
    {
        // TODO Auto-generated method stub
        return null;
    }

}
