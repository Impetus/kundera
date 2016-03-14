package com.impetus.dao;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import com.impetus.dao.utils.PropertyReader;

public class PersistenceService
{
    private static final String PU = "testPU";

    private static final String CLIENT_PROPERTIES = "client.properties";

    public static EntityManager getEM()
    {
        EntityManagerFactory emf = null;
		try {
			emf = Persistence.createEntityManagerFactory(PU,
			        PropertyReader.getProps(CLIENT_PROPERTIES));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

        EntityManager em = emf.createEntityManager();

        return em;
    }

}
