/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.kundera.client.crud.mappedsuperclass;

import java.util.Date;
import java.util.List;

import javax.persistence.AttributeOverride;
import javax.persistence.AttributeOverrides;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.MappedSuperclass;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author vivek.mishra junit for {@link MappedSuperclass},
 *         {@link AttributeOverride}, {@link AttributeOverrides}.
 */
public abstract class MappedSuperClassBase
{

    /** log for this class. */
    private static Logger log = LoggerFactory.getLogger(MappedSuperClassBase.class);

    protected String _PU = "corePu";

    /** The emf. */
    protected static EntityManagerFactory emf;

    /** The em. */
    protected static EntityManager em;

    protected void setUpInternal() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(_PU);
        em = emf.createEntityManager();
    }

    protected void assertInternal()
    {
        assertInternal(false);
    }

    protected void assertInternal(boolean wait)
    {
        CreditTransaction creditTx = new CreditTransaction();
        creditTx.setTxId("credit1");
        creditTx.setTxStatus(Status.APPROVED);
        creditTx.setBankIdentifier("sbi");
        creditTx.setTransactionDt(new Date());
        creditTx.setAmount(10);
        em.persist(creditTx);

        waitThread(wait);

        DebitTransaction debitTx = new DebitTransaction();
        debitTx.setTxId("debit1");
        debitTx.setTxStatus(Status.PENDING);
        debitTx.setTransactionDt(new Date());
        debitTx.setBankIdentifier("sbi");
        debitTx.setAmount(-10);
        em.persist(debitTx);

        waitThread(wait);
        em.clear();
        String creditQuery = "Select c from CreditTransaction c where c.bankIdentifier = 'sbi'";

        Query query = em.createQuery(creditQuery);

        List<CreditTransaction> results = query.getResultList();
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("credit1", results.get(0).getTxId());

        em.clear();
        String debitQuery = "Select d from DebitTransaction d where d.bankIdentifier = 'sbi'";

        query = em.createQuery(debitQuery);

        List<DebitTransaction> debitResults = query.getResultList();
        Assert.assertEquals(1, debitResults.size());
        Assert.assertEquals("debit1", debitResults.get(0).getTxId());

    }

    /**
     * Tear down.
     * 
     * @throws Exception
     *             the exception
     */
    protected void tearDownInternal() throws Exception
    {
        if (emf != null)
        {
            emf.close();
        }

        if (em != null)
        {
            em.close();
        }

        
    }

    private void waitThread(boolean toWait)
    {
        if (toWait)
        {
            try
            {
                Thread.sleep(2000);
            }
            catch (InterruptedException e)
            {
                log.error("Error while thread interruption, {}", e);
            }
        }
    }
}
