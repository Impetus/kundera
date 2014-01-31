/**
 * Copyright 2013 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera.metadata;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.query.Person;

/**
 * @author vivek.mishra junit for {@link MetadataUtils}.
 * 
 */
public class MetadataBuilderTest
{

    private String persistenceUnit = "patest";

    private EntityManagerFactory emf;

    private KunderaMetadata kunderaMetadata;

    @Before
    public void setup()
    {
        emf = Persistence.createEntityManagerFactory(persistenceUnit);
        kunderaMetadata = ((EntityManagerFactoryImpl)emf).getKunderaMetadataInstance();
    }

    @Test
    public void test()
    {
        MetadataBuilder metadataBuilder = new MetadataBuilder(persistenceUnit,
                "com.impetus.kundera.client.CoreTestClientFactory", null, kunderaMetadata);

        metadataBuilder.buildEntityMetadata(Person.class);
        metadataBuilder.validate(Person.class);

        Assert.assertNotNull(KunderaMetadataManager.getEntityMetadata(kunderaMetadata, Person.class));
    }

}
