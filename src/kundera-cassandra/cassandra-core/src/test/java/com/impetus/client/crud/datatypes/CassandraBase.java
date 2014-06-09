package com.impetus.client.crud.datatypes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.apache.thrift.TException;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;
import com.impetus.kundera.datatypes.datagenerator.DataGenerator;
import com.impetus.kundera.datatypes.datagenerator.DataGeneratorFactory;

public abstract class CassandraBase
{
    private static final String _PU = "CassandraDataTypeTest";

    public static final boolean RUN_IN_EMBEDDED_MODE = true;

    public static final boolean AUTO_MANAGE_SCHEMA = false;

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

        // just to create schema ones.

        propertyMap = new HashMap();
        propertyMap.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);

        if (propertyMap != null && !CassandraCli.isArchived(_PU))
        {
            propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create");
        }

        emf = Persistence.createEntityManagerFactory(_PU, propertyMap);

        CassandraCli.archivePu(_PU);
    }

    protected void tearDown() throws Exception
    {
        emf.close();
        dropSchema();
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

    public void startCluster()
    {
        try
        {
            CassandraCli.cassandraSetUp();
        }
        catch (IOException e)
        {

        }
        catch (TException e)
        {

        }
    }

    public void stopCluster()
    {
        // TODO Auto-generated method stub

    }

    // protected abstract void startCluster();
    //
    // protected abstract void stopCluster();

    protected abstract void createSchema();

    protected abstract void dropSchema();
}
