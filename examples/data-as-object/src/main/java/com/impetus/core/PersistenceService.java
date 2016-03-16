package com.impetus.core;

import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

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

    private static Map<String, Map<String, String>> entityConfigurations = Collections
            .synchronizedMap(new HashMap<String, Map<String, String>>());

    public static synchronized EntityManager getEM(EntityManagerFactory emf, final String propertiesPath,
            final String clazzName)
    {
        loadClientProperties(propertiesPath, clazzName);

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

    private static void loadClientProperties(String propertiesPath, String clazzName)
    {

        InputStream inputStream = PropertyReader.class.getClassLoader().getResourceAsStream(propertiesPath);
        clientProperties = JsonUtil.readJson(inputStream, Map.class);

        if (clientProperties != null)
        {
            if (clientProperties.get("all") != null)
            {
                entityConfigurations.put(clazzName, clientProperties.get("all"));
            }
            else
            {

                Iterator iter = clientProperties.keySet().iterator();

                while (iter.hasNext())
                {
                    Object key = iter.next();
                    if (((String) key).indexOf(',') > 0)
                    {
                        StringTokenizer tokenizer = new StringTokenizer((String) key, ",");
                        while (tokenizer.hasMoreElements())
                        {
                            String token = tokenizer.nextToken();
                            entityConfigurations.put(token, clientProperties.get(key));
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

}
