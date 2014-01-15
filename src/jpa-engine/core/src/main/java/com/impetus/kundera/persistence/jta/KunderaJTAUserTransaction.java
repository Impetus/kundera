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

import java.io.Serializable;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.UserTransaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.persistence.ResourceManager;

/**
 * Kundera implementation for JTA <code> UserTransaction</code>
 * 
 * This needs to hooked up with initial context for use of Kundera's
 * commit/rollback handling.
 * 
 * @author vivek.mishra@impetus.co.in
 * 
 */

public class KunderaJTAUserTransaction implements UserTransaction, Referenceable, Serializable
{

    /** The thread local. */
    private static transient ThreadLocal<KunderaTransaction> threadLocal = new ThreadLocal<KunderaTransaction>();

    /** The timer thead. */
    private static transient ThreadLocal<Integer> timerThead = new ThreadLocal<Integer>();

    /** The Constant DEFAULT_TIME_OUT. */
    private static final Integer DEFAULT_TIME_OUT = 60;

    /** The current tx. */
    private static transient KunderaJTAUserTransaction currentTx;

    /** The Constant log. */
    private static final Logger log = LoggerFactory.getLogger(KunderaJTAUserTransaction.class);

    /**
     * Instantiates a new kundera jta user transaction.
     */
    public KunderaJTAUserTransaction()
    {
        currentTx = this;
    }

    /**
     * Gets the current tx.
     * 
     * @return the current tx
     */
    public static KunderaJTAUserTransaction getCurrentTx()
    {
        return currentTx;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.transaction.UserTransaction#begin()
     */
    @Override
    public void begin() throws NotSupportedException, SystemException
    {
        if (log.isDebugEnabled())
            log.info("beginning JTA transaction");

        Transaction tx = threadLocal.get();
        if (tx != null)
        {
            if ((tx.getStatus() == Status.STATUS_MARKED_ROLLBACK))
            {
                throw new NotSupportedException("Nested Transaction not supported!");
            }
        }

        Integer timer = timerThead.get();
        threadLocal.set(new KunderaTransaction(timer != null ? timer : DEFAULT_TIME_OUT));
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.transaction.UserTransaction#commit()
     */
    @Override
    public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException,
            SecurityException, IllegalStateException, SystemException
    {
        Transaction tx = threadLocal.get();

        try
        {
            if (tx != null)
            {
                if (log.isDebugEnabled())
                    log.info("Commiting transaction:" + tx);
                tx.commit();
            }
            else
            {
                log.debug("Cannot locate a transaction to commit.");

            }
        }
        finally
        {
            if (log.isDebugEnabled())
                log.info("Resetting after commit.");
            threadLocal.set(null);
            timerThead.set(null);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.transaction.UserTransaction#getStatus()
     */
    @Override
    public int getStatus() throws SystemException
    {
        Transaction tx = threadLocal.get();
        if (tx == null)
        {
            return Status.STATUS_NO_TRANSACTION;
        }
        return tx.getStatus();
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.transaction.UserTransaction#rollback()
     */
    @Override
    public void rollback() throws IllegalStateException, SecurityException, SystemException
    {

        try
        {
            Transaction tx = threadLocal.get();
            if (tx == null)
            {
                throw new IllegalStateException("Cannot locate a Transaction for rollback.");
            }

            if (log.isDebugEnabled())
                log.info("Rollback transaction:" + tx);

            tx.rollback();

        }
        finally
        {
            if (log.isDebugEnabled())
                log.info("Resetting after rollback.");
            threadLocal.set(null);
            timerThead.set(null);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.transaction.UserTransaction#setRollbackOnly()
     */
    @Override
    public void setRollbackOnly() throws IllegalStateException, SystemException
    {
        Transaction tx = threadLocal.get();
        if (tx == null)
        {
            throw new IllegalStateException("Cannot get Transaction for setRollbackOnly");
        }
        tx.setRollbackOnly();

    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.transaction.UserTransaction#setTransactionTimeout(int)
     */
    @Override
    public void setTransactionTimeout(int timeout) throws SystemException
    {
        Transaction tx = threadLocal.get();
        if (tx == null)
        {
            timerThead.set(timeout);
        }
        else
        {
            if (log.isDebugEnabled())
                log.debug("Cannot reset running transaction:" + tx);
        }

    }

    /**
     * Returns transaction time out. If no timeout is associate with current
     * thread, returns default timeout(e.g. 60).
     * 
     * @return the transactionTimeout transaction timeout.
     */
    public int getTransactionTimeout()
    {
        Integer timeOut = timerThead.get();
        if (timeOut == null)
        {
            return DEFAULT_TIME_OUT;
        }
        return timeOut;
    }

    /**
     * Links referenced resource to current transaction thread.
     * 
     * @param implementor
     *            resource implementor.
     */
    public void setImplementor(ResourceManager implementor)
    {
        KunderaTransaction tx = threadLocal.get();
        if (tx == null)
        {
            throw new IllegalStateException("Cannot get Transaction to start");
        }
        tx.setImplementor(implementor);

    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.naming.Referenceable#getReference()
     */
    @Override
    public Reference getReference() throws NamingException
    {
        return UserTransactionFactory.getReference(this);
    }

}
