package com.impetus.client.crud.datatypes;

import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.kundera.datatypes.datagenerator.DataGenerator;
import com.impetus.kundera.datatypes.datagenerator.DataGeneratorFactory;

public abstract class CassandraBase
{
    public static final boolean RUN_IN_EMBEDDED_MODE = true;

    public static final boolean AUTO_MANAGE_SCHEMA = true;

    protected EntityManagerFactory emf;

    protected Map propertyMap;

    protected void setUp() throws Exception
    {
        if (RUN_IN_EMBEDDED_MODE)
        {
            startCluster();
        }
        if (AUTO_MANAGE_SCHEMA)
        {
            createSchema();
        }
        if (propertyMap == null)
        {
            propertyMap = new HashMap();
            propertyMap.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);
        }
        emf = Persistence.createEntityManagerFactory("CassandraDataTypeTest", propertyMap);
    }

    protected void tearDown() throws Exception
    {
        emf.close();
        if (AUTO_MANAGE_SCHEMA)
        {
            dropSchema();
        }
        if (RUN_IN_EMBEDDED_MODE)
        {
            stopCluster();
        }
    }

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

    protected abstract void startCluster();

    protected abstract void stopCluster();

    protected abstract void createSchema();

    protected abstract void dropSchema();
}
