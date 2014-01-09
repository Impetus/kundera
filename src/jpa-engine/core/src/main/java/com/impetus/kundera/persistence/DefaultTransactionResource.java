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

package com.impetus.kundera.persistence;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.persistence.api.Batcher;

/**
 * Default transaction implementation for databases who does not support
 * transactions. This can only ensure ATOMICITY out of ACID properties.
 * 
 * @author vivek.mishra
 */
public class DefaultTransactionResource implements TransactionResource
{

    private boolean isActive;

    private Client client;

    /** The Constant log. */
    private static final Logger log = LoggerFactory.getLogger(DefaultTransactionResource.class);

    private List<Node> nodes = new ArrayList<Node>();

    public DefaultTransactionResource(Client client)
    {
        this.client = client;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.persistence.TransactionResource#onBegin()
     */
    @Override
    public void onBegin()
    {
        isActive = true;

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.persistence.TransactionResource#onCommit()
     */
    @Override
    public void onCommit()
    {
        onFlush();
        nodes.clear();
        nodes = null;
        nodes = new ArrayList<Node>();
        isActive = false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.persistence.TransactionResource#onFlush()
     */
    public void onFlush()
    {
        for (Node node : nodes)
        {
            node.flush();
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.persistence.TransactionResource#onRollback()
     */
    @Override
    public void onRollback()
    {
        onBatchRollBack();

        nodes.clear();
        nodes = null;
        nodes = new ArrayList<Node>();
        isActive = false;
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

    /**
     * 
     * @param node
     * @param events
     */
    void syncNode(Node node)
    {
        nodes.add(node);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.persistence.TransactionResource#isActive()
     */
    @Override
    public boolean isActive()
    {
        return isActive;
    }

    /**
     * In case of rollback, clear added batch, if any.
     */
    private void onBatchRollBack()
    {
        if (client instanceof Batcher)
        {
            ((Batcher) client).clear();
        }

    }

}
