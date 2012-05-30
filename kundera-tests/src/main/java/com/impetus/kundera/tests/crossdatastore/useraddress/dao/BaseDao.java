package com.impetus.kundera.tests.crossdatastore.useraddress.dao;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

public class BaseDao
{

    EntityManagerFactory emf;

    EntityManager em;

    public EntityManager getEntityManager(String pu)
    {
        if (emf == null)
        {
            emf = Persistence.createEntityManagerFactory(pu);
            em = emf.createEntityManager();
        }

        if(em == null) {
            em = emf.createEntityManager();
        }              

        return em;
    }

    public void closeEntityManager()
    {
        if (em != null)
        {
            em.close();
            em = null;
        }          
    }
    
    public void closeEntityManagerFactory() {
    	if(emf != null) {
    		emf.close();
    	}
    	emf = null;
    }
    
    

}
