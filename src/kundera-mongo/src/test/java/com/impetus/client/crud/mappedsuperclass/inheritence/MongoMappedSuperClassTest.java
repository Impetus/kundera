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
package com.impetus.client.crud.mappedsuperclass.inheritence;

import java.util.Date;
import java.util.List;

import javax.persistence.AttributeOverride;
import javax.persistence.AttributeOverrides;
import javax.persistence.MappedSuperclass;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.utils.MongoUtils;
import com.impetus.kundera.client.crud.mappedsuperclass.CreditTransaction;
import com.impetus.kundera.client.crud.mappedsuperclass.MappedSuperClassBase;
import com.impetus.kundera.client.crud.mappedsuperclass.Status;
import com.impetus.kundera.client.mongo.mappedsuperclass.Ledger;



/**
 * @author vivek.mishra junit for {@link MappedSuperclass},
 *         {@link AttributeOverride}, {@link AttributeOverrides}.
 */
public class MongoMappedSuperClassTest extends MappedSuperClassBase
{

    @Before
    public void setUp() throws Exception
    {
        
        _PU = "mongoTest";
        setUpInternal();
    }

    @Test
    public void test()
    {
        assertInternal();
        
    }
    
    @Test
    public void testRelation()
    {
        CreditTransaction creditTx = new CreditTransaction();
        creditTx.setTxId("credit1");
        creditTx.setTxStatus(Status.APPROVED);
        creditTx.setBankIdentifier("sbi");
        creditTx.setTransactionDt(new Date());
        creditTx.setAmount(10);
        
        Ledger ledger = new Ledger();
        ledger.setLedgerId("l1");
        ledger.setPayee("User1");
        
        creditTx.setLedger(ledger);
        
//        ledger.setTransaction(creditTx);
        em.persist(creditTx);
     
           
        em.clear();
        String creditQuery = "Select c from CreditTransaction c where c.bankIdentifier = 'sbi'";

        Query query = em.createQuery(creditQuery);

        List<CreditTransaction> results = query.getResultList();
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("credit1", results.get(0).getTxId());
        Assert.assertNotNull(results.get(0).getLedger());
        
    
        em.clear();
      

    }

    
    

    /**
     * Tear down.
     * 
     * @throws Exception
     *             the exception
     */
    @After
    public void tearDown() throws Exception
    {
        MongoUtils.dropDatabase(emf, _PU);
        tearDownInternal();
    }

}
