/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
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

package com.impetus.client.redis;

import java.util.ArrayList;
import java.util.List;

import redis.clients.jedis.BinaryTransaction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import com.impetus.kundera.persistence.TransactionResource;

/**
 * Defines transaction boundaries for redis client, if user opted for
 * transaction support(kundera.transaction.resource)
 * 
 * @author vivek
 * 
 */
public class RedisTransaction implements TransactionResource
{

    private List<Transaction> resources = new ArrayList<Transaction>();

    private boolean isTransactionInProgress;

    /**
     * Default constructor
     */
    public RedisTransaction()
    {

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.persistence.TransactionResource#onBegin()
     */
    @Override
    public void onBegin()
    {
        isTransactionInProgress = true;

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.persistence.TransactionResource#onCommit()
     */
    @Override
    public void onCommit()
    {
        for (BinaryTransaction resource : resources)
        {
            resource.exec();
        }
        isTransactionInProgress = false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.persistence.TransactionResource#onRollback()
     */
    @Override
    public void onRollback()
    {
        for (BinaryTransaction resource : resources)
        {
            resource.discard();
        }

        resources.clear();
        resources = new ArrayList<Transaction>();
        isTransactionInProgress = false;

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.persistence.TransactionResource#onFlush()
     */
    @Override
    public void onFlush()
    {
        onCommit();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.persistence.TransactionResource#prepare()
     */
    @Override
    public Response prepare()
    {
        return Response.YES;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.persistence.TransactionResource#isActive()
     */
    @Override
    public boolean isActive()
    {
        return isTransactionInProgress;
    }

    Transaction bindResource(Jedis resource)
    {
        Transaction tx = null;
        if (resources.isEmpty())
        {
            tx = resource.multi();
            resources.add(tx);
        }
        else
        {
            tx = resources.get(0);
        }
        return tx;
    }
}
