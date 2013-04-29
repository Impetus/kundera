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

import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;

/**
 * Class implements <code>EntityTransaction </code> interface. It implements
 * begin/commit/roll back and other methods.
 * 
 * @author vivek.mishra
 * 
 */
public class KunderaEntityTransaction implements EntityTransaction
{
    private EntityManager entityManager;

    private Coordinator coordinator;

    private Boolean rollbackOnly;

    enum TxAction
    {
        BEGIN, COMMIT, ROLLBACK, PREPARE;
    }

    KunderaEntityTransaction(EntityManager entityManager)
    {
        this.entityManager = entityManager;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityTransaction#begin()
     */
    @Override
    public void begin()
    {
        if (isActive())
        {
            throw new IllegalStateException("Transaction is already active");
        }
        else
        {
            this.coordinator = ((EntityManagerImpl) entityManager).getPersistenceDelegator().getCoordinator();
            ((EntityManagerImpl) entityManager).getPersistenceDelegator().begin(); // transaction
                                                                                   // de-marcation.
            this.coordinator.coordinate(TxAction.BEGIN);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityTransaction#commit()
     */
    @Override
    public void commit()
    {
        if (!getRollbackOnly())
        {
            onTransaction(TxAction.COMMIT);
            ((EntityManagerImpl) entityManager).getPersistenceDelegator().commit();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityTransaction#getRollbackOnly()
     */
    @Override
    public boolean getRollbackOnly()
    {
        if (isActive())
        {
            return rollbackOnly != null ? rollbackOnly : false;
        }
        else
        {
            throw new IllegalStateException("No transaction in progress");
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityTransaction#isActive()
     */
    @Override
    public boolean isActive()
    {
        return (coordinator != null && coordinator.isTransactionActive());
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityTransaction#rollback()
     */
    @Override
    public void rollback()
    {
        onTransaction(TxAction.ROLLBACK);
        ((EntityManagerImpl) entityManager).getPersistenceDelegator().rollback();
    }

    private void onTransaction(TxAction action)
    {
        if (isActive())
        {
            coordinator.coordinate(action);
        }
        else
        {
            throw new IllegalStateException("No transaction in progress");
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityTransaction#setRollbackOnly()
     */
    @Override
    public void setRollbackOnly()
    {
        this.rollbackOnly = true;
    }
}
