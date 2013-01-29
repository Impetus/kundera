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
package com.impetus.client.neo4j;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import com.impetus.kundera.persistence.TransactionResource;

/**
 * Defines transaction boundaries for Neo4J client, in case  
 * user opts for transaction support (kundera.transaction.resource)
 * @author amresh.singh
 */
public class Neo4JTransaction implements TransactionResource
{
    private boolean isTransactionInProgress;  
    
    GraphDatabaseService graphDb = null;
    Transaction tx = null;
    
    @Override
    public void onBegin()
    {
        if(graphDb != null && ! isTransactionInProgress)
        {
            tx = graphDb.beginTx();
        }
        
        isTransactionInProgress = true;
    }

    @Override
    public void onCommit()
    {
        if(tx != null && isTransactionInProgress)
        {
            tx.success();
            tx.finish();
        }
        
        isTransactionInProgress = false;
    }

    @Override
    public void onRollback()
    {
        if(tx != null && isTransactionInProgress)
        {
            tx.failure();
            tx.finish();
        }
        
        
        tx = null;
        isTransactionInProgress = false;
    }

    @Override
    public void onFlush()
    {
        onCommit();
    }

    @Override
    public Response prepare()
    {
        return Response.YES;
    }

    @Override
    public boolean isActive()
    {
        return isTransactionInProgress;
    }

    /**
     * @return the graphDb
     */
    public GraphDatabaseService getGraphDb()
    {
        return graphDb;
    }

    /**
     * @param graphDb the graphDb to set
     */
    public void setGraphDb(GraphDatabaseService graphDb)
    {
        this.graphDb = graphDb;
    } 

}
