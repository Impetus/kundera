package com.impetus.dao;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import com.impetus.dao.utils.PropertyReader;

public class PersistenceService
{
    private static final String PU = "testPU";

    private static EntityManagerFactory emf;

    private static EntityManager em;

    public static synchronized EntityManager getEM(final String propertiesPath)
    {
        if (emf == null)
        {
            try
            {
                emf = Persistence.createEntityManagerFactory(PropertyReader.getProps(propertiesPath).getProperty("pu"),
                        PropertyReader.getProps(propertiesPath));

            }
            catch (Exception e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        em = emf.createEntityManager();

        return em;
    }

}
