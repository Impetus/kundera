package com.impetus.client.crud.datatypes;

import java.lang.reflect.Field;
import java.util.Map;

import javax.persistence.EntityManager;

import com.impetus.client.mongodb.MongoDBClient;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.datatypes.datagenerator.DataGenerator;
import com.impetus.kundera.datatypes.datagenerator.DataGeneratorFactory;
import com.mongodb.DB;

public abstract class MongoBase
{
    public static final boolean RUN_IN_EMBEDDED_MODE = false;

    public static final boolean AUTO_MANAGE_SCHEMA = false;
    
    protected static final String PERSISTENCE_UNIT = "MongoDataTypeTest";

    DataGenerator<?> dataGenerator;

    DataGeneratorFactory factory = new DataGeneratorFactory();

    protected Object getMaxValue(Class<?> clazz)
    {
        dataGenerator = factory.getDataGenerator(clazz);
        return dataGenerator.maxValue();
    }

    protected Object getMinValue(Class<?> clazz)
    {
        dataGenerator = factory.getDataGenerator(clazz);
        return dataGenerator.minValue();
    }

    protected Object getRandomValue(Class<?> clazz)
    {
        dataGenerator = factory.getDataGenerator(clazz);
        return dataGenerator.randomValue();
    }

    protected Object getPartialValue(Class<?> clazz)
    {
        dataGenerator = factory.getDataGenerator(clazz);
        return dataGenerator.partialValue();
    }

    /**
     * 
     */
    protected void truncateMongo(EntityManager em, final String persistenceUnit, final String tableName)
    {
        Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
        MongoDBClient client = (MongoDBClient) clients.get(persistenceUnit);
        if(client != null)
        {
            try
            {
                Field db = client.getClass().getDeclaredField("mongoDb");
                if(!db.isAccessible())
                {
                    db.setAccessible(true);
                }
                DB mongoDB =  (DB) db.get(client);
                mongoDB.getCollection(tableName).drop();
            }
            catch (SecurityException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            catch (NoSuchFieldException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            catch (IllegalArgumentException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            catch (IllegalAccessException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        
    }


    protected abstract void startCluster();

    protected abstract void stopCluster();

    protected abstract void createSchema();

    protected abstract void dropSchema();
}
