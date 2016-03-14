package com.impetus.dao;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.dao.utils.PropertyReader;

public class PersistenceService
{

    private static Logger LOGGER = LoggerFactory.getLogger(PersistenceService.class);

    private static final String PU = "testPU";

    Map<List<String>, Map<String, String>> clientProperiesJson = new HashMap<List<String>, Map<String, String>>();

    private static EntityManagerFactory emf;

    private static EntityManager em;

    public static synchronized EntityManager getEM(final String propertiesPath)
    {
        if (emf == null)
        {
            try
            {
                emf = Persistence.createEntityManagerFactory("testPU", PropertyReader.getProps(propertiesPath));

            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }

        em = emf.createEntityManager();

        return em;
    }

}
