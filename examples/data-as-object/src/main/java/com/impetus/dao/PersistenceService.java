package com.impetus.dao;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.dao.utils.JsonUtil;
import com.impetus.dao.utils.PropertyReader;
import com.impetus.kundera.KunderaException;

public class PersistenceService
{

    private static Logger LOGGER = LoggerFactory.getLogger(PersistenceService.class);

    private static Map<?, Map<String, String>> clientProperties = new HashMap<>();

    private static Map<String, Map<String, String>> entityConfigurations = new HashMap<>();

    public static synchronized EntityManager getEM(EntityManagerFactory emf, final String propertiesPath,
            final String clazzName)
    {
        loadClientProperties(propertiesPath);

        if (emf == null)
        {
            try
            {
                emf = Persistence.createEntityManagerFactory("testPU", entityConfigurations.get(clazzName));

            }
            catch (Exception e)
            {
                LOGGER.error("Unable to create Entity Manager Factory. Caused By: ", e);
                throw new KunderaException("Unable to create Entity Manager Factory. Caused By: ", e);
            }
        }

        return emf.createEntityManager();
    }

    private static void loadClientProperties(String propertiesPath)
    {

            InputStream inputStream = PropertyReader.class.getClassLoader().getResourceAsStream(propertiesPath);
            clientProperties = JsonUtil.readJson(inputStream, Map.class);

            if (clientProperties != null)
            {
                Iterator iter = clientProperties.keySet().iterator();

                while (iter.hasNext())
                {
                    Object key = iter.next();
                    if (key.getClass().isAssignableFrom(List.class))
                    {
                        Iterator i = ((List) key).iterator();
                        while (i.hasNext())
                        {
                            String entity = (String) i.next();
                            entityConfigurations.put(entity, clientProperties.get(key));
                        }
                    }
                    else
                    {
                        entityConfigurations.put((String) key, clientProperties.get(key));
                    }
                }
            }

    }

}
