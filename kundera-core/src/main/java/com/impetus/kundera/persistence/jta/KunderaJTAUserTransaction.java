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
import javax.transaction.UserTransaction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.KunderaException;
import com.impetus.kundera.persistence.ResourceManager;

/**
 * Kundera implementation for JTA {@link UserTransaction}. This needs to hooked
 * up with initial context for use of Kundera's commit/rollback handling.
 * 
 * @author vivek.mishra
 * 
 */

public class KunderaJTAUserTransaction implements UserTransaction, Referenceable, Serializable
{
    private boolean isTransactionInProgress;

    private boolean setRollBackOnly;

//    private java.util.List<ResourceManager> implementors;
    
    private static transient ThreadLocal<KunderaTransaction> threadLocal = new ThreadLocal<KunderaTransaction>();
    
    private int transactionTimeout;

//    private int status = Status.STATUS_NO_TRANSACTION;
    
    private static transient KunderaJTAUserTransaction currentTx;
    
    public KunderaJTAUserTransaction()
    {
        currentTx = this;
    }


    public static KunderaJTAUserTransaction getCurrentTx()
    {
        return currentTx;
    }
    
    /** The Constant log. */
    private static final Log log = LogFactory.getLog(KunderaJTAUserTransaction.class);

    /*
     * (non-Javadoc)
     * 
     * @see javax.transaction.UserTransaction#begin()
     */
    @Override
    public void begin() throws NotSupportedException, SystemException
    {
        isTransactionInProgress = true;
//        status = Status.STATUS_ACTIVE;
        threadLocal.set(new KunderaTransaction());
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
        threadLocal.get().commit();
        threadLocal.set(null);
        
//        if (isTransactionInProgress)
//        {
////            status = Status.STATUS_COMMITTING;
//            // Do commit!
//            if (implementors != null)
//            {
//                if (!setRollBackOnly)
//                {
//                    for(ResourceManager implementor: implementors)
//                    {
//                        implementor.doCommit();
//                    }
//                }
//                else
//                {
//                    for (ResourceManager implementor : implementors)
//                    {
//                        implementor.doRollback();
//                    }
//                }
//            }
//        }
//        else
//        {
//
//            throw new KunderaException("No transaction in progress.");
//        }
//        status = Status.STATUS_COMMITTED;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.transaction.UserTransaction#getStatus()
     */
    @Override
    public int getStatus() throws SystemException
    {
        KunderaTransaction tx = threadLocal.get();
        if(tx ==null)
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
        if (isTransactionInProgress)
        {
            threadLocal.get().rollback();
            
//            if (implementors != null)
//            {
//                for(ResourceManager implementor: implementors)
//                {
//                    implementor.doRollback();
//                }
//            }
        }
        else
        {

            throw new KunderaException("No transaction in progress.");
        }
//        status = Status.STATUS_ROLLEDBACK;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.transaction.UserTransaction#setRollbackOnly()
     */
    @Override
    public void setRollbackOnly() throws IllegalStateException, SystemException
    {
        threadLocal.get().setRollbackOnly();
//        setRollBackOnly = true;
//        status = Status.STATUS_MARKED_ROLLBACK;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.transaction.UserTransaction#setTransactionTimeout(int)
     */
    @Override
    public void setTransactionTimeout(int arg0) throws SystemException
    {
        this.transactionTimeout = arg0;
    }
    
    
    /**
     * 
     * @param implementor
     */
    public void setImplementor(ResourceManager implementor)
    {
        threadLocal.get().setImplementor(implementor);
        
//        if(implementors == null)
//        {
//            implementors = new ArrayList<ResourceManager>();
//        }
//        implementors.add(implementor);
    }

    /* (non-Javadoc)
     * @see javax.naming.Referenceable#getReference()
     */
    @Override
    public Reference getReference() throws NamingException
    {
        return UserTransactionFactory.getReference(this);
    }
    
}
