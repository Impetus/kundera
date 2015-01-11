/*******************************************************************************
 * * Copyright 2015 Impetus Infotech.
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
package com.impetus.kundera.client.crud.associations;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

/**
 * @author Pragalbh Garg
 *
 */
public abstract class AssociationsBiDirectionalBase
{

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
    
    protected void assertBegin(){
        init();
        
        String qry = "select m from MobileHandset m";
        Query query = em.createQuery(qry);
        List<MobileHandset> result = query.getResultList();
        Assert.assertNotNull(result);
        Assert.assertNotNull(result.get(0));
        Assert.assertNotNull(result.get(0).getManufacturer());
        Assert.assertNotNull(result.get(0).getOs());
        
        Assert.assertNotNull(result.get(0).getManufacturer().getHandsets());
        Assert.assertNotNull(result.get(0).getOs().getHandsets());

        MobileHandset mobile = em.find(MobileHandset.class, "m1");
        Assert.assertNotNull(mobile);
        Assert.assertEquals("manufacturer1",mobile.getManufacturer().getName());
    }

    protected void init()
    {
        MobileManufacturer manu1 = new MobileManufacturer();
        manu1.setId("ma1");
        manu1.setName("manufacturer1");

        MobileManufacturer manu2 = new MobileManufacturer();
        manu2.setId("ma2");
        manu2.setName("manufacturer2");

        MobileOperatingSystem os1 = new MobileOperatingSystem();
        os1.setId("o1");
        os1.setName("os1");

        MobileOperatingSystem os2 = new MobileOperatingSystem();
        os2.setId("o2");
        os2.setName("os2");

        MobileHandset mobile1 = new MobileHandset();
        mobile1.setId("m1");
        mobile1.setName("mobile1");
        mobile1.setManufacturer(manu1);
        mobile1.setOs(os1);

        MobileHandset mobile2 = new MobileHandset();
        mobile2.setId("m2");
        mobile2.setName("mobile2");
        mobile2.setManufacturer(manu1);
        mobile2.setOs(os1);

        MobileHandset mobile3 = new MobileHandset();
        mobile3.setId("m3");
        mobile3.setName("mobile3");
        mobile3.setManufacturer(manu1);
        mobile3.setOs(os2);

        MobileHandset mobile4 = new MobileHandset();
        mobile4.setId("m4");
        mobile4.setName("mobile4");
        mobile4.setManufacturer(manu2);
        mobile4.setOs(os1);

        em.persist(mobile1);
        em.persist(mobile2);
        em.persist(mobile3);
        em.persist(mobile4);

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

}
