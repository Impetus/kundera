package com.impetus.client.crud.datatypes;

import java.sql.SQLException;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import com.impetus.client.crud.RDBMSCli;
import com.impetus.kundera.datatypes.datagenerator.DataGenerator;
import com.impetus.kundera.datatypes.datagenerator.DataGeneratorFactory;
import com.impetus.kundera.metadata.model.KunderaMetadata;

public abstract class RdbmsBase
{
    public static final boolean AUTO_MANAGE_SCHEMA = true;

    protected EntityManagerFactory emf;

    protected RDBMSCli cli;

    protected void setUp() throws Exception
    {
        cli = new RDBMSCli("TESTDB");
        cli.createSchema("TESTDB");

        KunderaMetadata.INSTANCE.setApplicationMetadata(null);
        if (AUTO_MANAGE_SCHEMA)
        {
            createSchema();
        }
        emf = Persistence.createEntityManagerFactory("RdbmsDataTypeTest");
    }

    protected void tearDown() throws Exception
    {
        emf.close();
        if (AUTO_MANAGE_SCHEMA)
        {
            dropSchema();
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

    protected abstract void createSchema() throws SQLException;

    protected abstract void dropSchema();
}
