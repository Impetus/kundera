package com.impetus.client.couchdb.datatypes.tests;

import org.apache.http.HttpHost;
import org.apache.http.client.HttpClient;

import com.impetus.client.couchdb.CouchDBUtils;
import com.impetus.client.couchdb.utils.CouchDBTestUtils;
import com.impetus.kundera.datatypes.datagenerator.DataGenerator;
import com.impetus.kundera.datatypes.datagenerator.DataGeneratorFactory;

public abstract class CouchDBBase
{

    protected HttpClient httpClient;

    protected HttpHost httpHost;

    protected String pu = "couchdb_pu";

    public static final boolean RUN_IN_EMBEDDED_MODE = true;

    public static final boolean AUTO_MANAGE_SCHEMA = true;

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

    protected void setUpBase()
    {
        httpClient = CouchDBTestUtils.initiateHttpClient(pu);
        httpHost = new HttpHost("localhost", 5984);
    }

    protected void createDatabase()
    {
        CouchDBTestUtils.createDatabase("couchdatabase", httpClient, httpHost);
    }

    protected void dropDatabase()
    {
        CouchDBTestUtils.dropDatabase("couchdatabase", httpClient, httpHost);
    }
}
