package com.impetus.kundera.persistence.jta;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;

import com.impetus.kundera.persistence.ResourceManager;

/**
 * @author vivek.mishra
 *
 */
public class KunderaTransaction implements Transaction
{

    private ResourceManager implementor;
    
    private boolean setRollBackOnly;
    
    private int status = Status.STATUS_ACTIVE;

//    public Kundera
    /* (non-Javadoc)
     * @see javax.transaction.Transaction#commit()
     */
    @Override
    public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException,
            SecurityException, IllegalStateException, SystemException
    {
        if (!setRollBackOnly)
        {
            if (implementor != null)
            {
                implementor.doCommit();
                status = Status.STATUS_COMMITTED;
            }
        }
        else
        {
            if (implementor != null)
            {
                implementor.doRollback();
                status = Status.STATUS_ROLLEDBACK;
            }
        }

    }

    /* (non-Javadoc)
     * @see javax.transaction.Transaction#delistResource(javax.transaction.xa.XAResource, int)
     */
    @Override
    public boolean delistResource(XAResource paramXAResource, int paramInt) throws IllegalStateException,
            SystemException
    {
        // TODO Auto-generated method stub
        return false;
    }

    /* (non-Javadoc)
     * @see javax.transaction.Transaction#enlistResource(javax.transaction.xa.XAResource)
     */
    @Override
    public boolean enlistResource(XAResource paramXAResource) throws RollbackException, IllegalStateException,
            SystemException
    {
        // TODO Auto-generated method stub
        return false;
    }

    /* (non-Javadoc)
     * @see javax.transaction.Transaction#getStatus()
     */
    @Override
    public int getStatus() throws SystemException
    {
        // TODO Auto-generated method stub
        return status;
    }

    /* (non-Javadoc)
     * @see javax.transaction.Transaction#registerSynchronization(javax.transaction.Synchronization)
     */
    @Override
    public void registerSynchronization(Synchronization paramSynchronization) throws RollbackException,
            IllegalStateException, SystemException
    {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see javax.transaction.Transaction#rollback()
     */
    @Override
    public void rollback() throws IllegalStateException, SystemException
    {
        if (implementor != null)
        {
            implementor.doRollback();
            status = Status.STATUS_ROLLEDBACK;
        }
    }

    /* (non-Javadoc)
     * @see javax.transaction.Transaction#setRollbackOnly()
     */
    @Override
    public void setRollbackOnly() throws IllegalStateException, SystemException
    {
        setRollBackOnly = true;

    }

    void setImplementor(ResourceManager implementor)
    {
        this.implementor = implementor;
    }
}
