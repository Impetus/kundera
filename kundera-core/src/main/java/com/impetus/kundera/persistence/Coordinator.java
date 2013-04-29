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

import java.util.HashMap;
import java.util.Map;

import com.impetus.kundera.persistence.KunderaEntityTransaction.TxAction;
import com.impetus.kundera.persistence.TransactionResource.Response;

/**
 * @author vivek
 * 
 */
class Coordinator
{

    // private List<TransactionResource> txResources = new
    // ArrayList<TransactionResource>();

    private Map<String, TransactionResource> txResources = new HashMap<String, TransactionResource>();

    public Coordinator()
    {

    }

    void addResource(TransactionResource resource, final String pu)
    {
        txResources.put(pu, resource);
    }

    TransactionResource getResource(final String pu)
    {
        return txResources.get(pu);
    }

    Response coordinate(TxAction action)
    {
        Response response = Response.YES;
        switch (action)
        {
        case BEGIN:
            for (TransactionResource res : txResources.values())
            {
                res.onBegin();
            }
            break;

        case PREPARE:

            // TODO:: need to handle case of two phase commit, in case of
            // polyglot persistence.

            for (TransactionResource res : txResources.values())
            {
                res.prepare();
            }
            break;

        case COMMIT:

            for (TransactionResource res : txResources.values())
            {
                res.onCommit();
            }
            break;

        case ROLLBACK:
            for (TransactionResource res : txResources.values())
            {
                res.onRollback();
            }

            break;

        default:
            throw new IllegalArgumentException("Invalid transaction action : " + action);
        }

        return response;
    }

    boolean isTransactionActive()
    {
        for (TransactionResource res : txResources.values())
        {
            if (res.isActive())
            {
                return true;
            }

        }
        return false;
    }
}
