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
package com.impetus.kundera.persistence.jta;

import java.util.HashSet;
import java.util.Set;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.persistence.ResourceManager;

/**
 * Implementation for <code> javax.transaction.Transaction </code>
 * 
 * @author vivek.mishra
 * 
 */
public class KunderaTransaction implements Transaction
{

    private Set<ResourceManager> implementors = new HashSet<ResourceManager>();

    private boolean setRollBackOnly;

    private int status = Status.STATUS_ACTIVE;

    /** The time out in millis. */
    private int timeOutInMillis;

    /** The Constant log. */
    private static final Logger log = LoggerFactory.getLogger(KunderaTransaction.class);

    /**
     * Default constructor with timeout parameter.
     */
    KunderaTransaction(int timeOutInMillis)
    {
        this.timeOutInMillis = timeOutInMillis;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.transaction.Transaction#commit()
     */
    @Override
    public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException,
            SecurityException, IllegalStateException, SystemException
    {
        if (!setRollBackOnly)
        {
            for (ResourceManager implementor : implementors)
            {
                if (implementor != null)
                {
                    implementor.doCommit();
                }
            }
            status = Status.STATUS_COMMITTED;
        }
        else
        {
            if (log.isDebugEnabled())
                log.debug("Transaction is set for rollback only, processing rollback.");

            for (ResourceManager implementor : implementors)
            {
                if (implementor != null)
                {
                    implementor.doRollback();
                    status = Status.STATUS_ROLLEDBACK;
                }
            }
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.transaction.Transaction#delistResource(javax.transaction.xa.XAResource
     * , int)
     */
    @Override
    public boolean delistResource(XAResource paramXAResource, int paramInt) throws IllegalStateException,
            SystemException
    {
        // TODD: need to look into.
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.transaction.Transaction#enlistResource(javax.transaction.xa.XAResource
     * )
     */
    @Override
    public boolean enlistResource(XAResource paramXAResource) throws RollbackException, IllegalStateException,
            SystemException
    {
        // TODD: need to look into.
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.transaction.Transaction#getStatus()
     */
    @Override
    public int getStatus() throws SystemException
    {
        return status;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.transaction.Transaction#registerSynchronization(javax.transaction
     * .Synchronization)
     */
    @Override
    public void registerSynchronization(Synchronization paramSynchronization) throws RollbackException,
            IllegalStateException, SystemException
    {
        throw new UnsupportedOperationException("Currently it is not supported.");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.transaction.Transaction#rollback()
     */
    @Override
    public void rollback() throws IllegalStateException, SystemException
    {
        for (ResourceManager implementor : implementors)
        {
            if (implementor != null)
            {
                implementor.doRollback();
                status = Status.STATUS_ROLLEDBACK;
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.transaction.Transaction#setRollbackOnly()
     */
    @Override
    public void setRollbackOnly() throws IllegalStateException, SystemException
    {
        setRollBackOnly = true;
        status = Status.STATUS_MARKED_ROLLBACK;
    }

    void setImplementor(ResourceManager implementor)
    {
        implementors.add(implementor);
    }

    /**
     * @return the transactionTimeout
     */
    public int getTransactionTimeout()
    {
        return timeOutInMillis;
    }

}
