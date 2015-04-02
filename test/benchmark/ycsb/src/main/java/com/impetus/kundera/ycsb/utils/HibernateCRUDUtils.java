/**
 * Copyright 2012 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera.ycsb.utils;

import java.util.Date;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;
import javax.persistence.TemporalType;

/**
 * @author Kuldeep Mishra
 * 
 */
public class HibernateCRUDUtils
{
    private EntityManagerFactory emf;

    public void persistInfo(com.impetus.kundera.ycsb.entities.PerformanceNoInfo info)
    {
        // create emf and em.
        EntityManager em = getEntityManager();

        em.getTransaction().begin();

        em.persist(info);

        em.getTransaction().commit();
        closeEntityManager(em);
    }

    /**
     * @return
     * 
     */
    private EntityManager getEntityManager()
    {
        if (emf == null || !emf.isOpen())
        {
            emf = Persistence.createEntityManagerFactory("kundera_rdbms_pu");
        }
        return emf.createEntityManager();
    }

    private void closeEntityManager(EntityManager em)
    {
        em.close();
    }

    /**
     * @param testType
     * 
     */
    public int getMaxRunSequence(Date date, String testType)
    {
        EntityManager em = getEntityManager();

        em.getTransaction().begin();

        Query q = em.createQuery("select max(p.runSequence) from PerformanceNoInfo p where p.date = '"+date+"'");
     //   q.setParameter("date", date, TemporalType.DATE);
        List results = q.getResultList();

        em.getTransaction().commit();

        closeEntityManager(em);

        return results.get(0) != null ? (Integer) results.get(0) : 0;
    }

    
    public static void main(String[] args)
    {
        HibernateCRUDUtils utils = new HibernateCRUDUtils();
        
        EntityManager em  = utils.getEntityManager();
        
        System.out.println(em.getProperties());
    }
}
