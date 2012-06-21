package com.impetus.kundera.persistence.jta;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.UserTransaction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.KunderaException;

/**
 * Kundera implementation for JTA {@link UserTransaction}. 
 * This needs to hooked up with initial context for use of Kundera's commit/rollback handling.
 * @author vivek.mishra
 * 
 */
public class KunderaJTATransaction implements UserTransaction
{
    private boolean isTransactionInProgress;

    /** The Constant log. */
    private static final Log log = LogFactory.getLog(KunderaJTATransaction.class);

    /*
     * (non-Javadoc)
     * 
     * @see javax.transaction.UserTransaction#begin()
     */
    @Override
    public void begin() throws NotSupportedException, SystemException
    {
        isTransactionInProgress = true;
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
        if (isTransactionInProgress)
        {
            // Do commit!
        }
        else
        {

            throw new KunderaException("No transaction in progress.");
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
        // TODO Auto-generated method stub
        return 0;
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
            // Do commit!
        }
        else
        {

            throw new KunderaException("No transaction in progress.");
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
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.transaction.UserTransaction#setTransactionTimeout(int)
     */
    @Override
    public void setTransactionTimeout(int arg0) throws SystemException
    {
        // TODO Auto-generated method stub

    }

}
