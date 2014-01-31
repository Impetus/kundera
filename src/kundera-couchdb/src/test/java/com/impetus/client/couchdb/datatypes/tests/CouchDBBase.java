/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/
package com.impetus.client.couchdb.datatypes.tests;

import org.apache.http.HttpHost;
import org.apache.http.client.HttpClient;

import com.impetus.client.couchdb.utils.CouchDBTestUtils;
import com.impetus.kundera.datatypes.datagenerator.DataGenerator;
import com.impetus.kundera.datatypes.datagenerator.DataGeneratorFactory;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;

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

    protected void setUpBase(final KunderaMetadata kunderaMetadata)
    {
        httpClient = CouchDBTestUtils.initiateHttpClient(kunderaMetadata, pu);
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
