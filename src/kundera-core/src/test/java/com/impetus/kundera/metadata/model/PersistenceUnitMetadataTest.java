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
package com.impetus.kundera.metadata.model;

import java.net.URL;
import java.util.Enumeration;
import java.util.List;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.kundera.loader.PersistenceXMLLoader;

/**
 * @author kuldeep.mishra
 * junit for {@link PersistenceUnitMetadata}
 *
 */
public class PersistenceUnitMetadataTest
{
    private static List<PersistenceUnitMetadata> metadatas;

    @BeforeClass
    public static void setUp() throws Exception
    {
        Enumeration<URL> xmls = PersistenceUnitMetadata.class.getClassLoader().getResources("META-INF/persistence.xml");

        while (xmls.hasMoreElements())
        {
            metadatas = PersistenceXMLLoader.findPersistenceUnits(xmls.nextElement());
        }
    }

    @AfterClass
    public static void tearDown() throws Exception
    {
        metadatas = null;
    }

    @Test
    public void testGetPersistenceUnitName()
    {
        Assert.assertNotNull(metadatas);
        Assert.assertFalse(metadatas.isEmpty());
        Assert.assertNotNull(metadatas.get(0));
        Assert.assertEquals("kunderatest", metadatas.get(0).getPersistenceUnitName());
    }

    @Test
    public void testGetPersistenceProviderClassName()
    {
        Assert.assertNotNull(metadatas);
        Assert.assertFalse(metadatas.isEmpty());
        Assert.assertNotNull(metadatas.get(0));
        Assert.assertEquals("com.impetus.kundera.KunderaPersistence", metadatas.get(0)
                .getPersistenceProviderClassName());
    }

    @Test
    public void testGetPersistenceXMLSchemaVersion()
    {
        Assert.assertNotNull(metadatas);
        Assert.assertFalse(metadatas.isEmpty());
        Assert.assertNotNull(metadatas.get(0));
        Assert.assertEquals("2.0", metadatas.get(0).getPersistenceXMLSchemaVersion());
    }

    @Test
    public void testGetClassLoader()
    {
        Assert.assertNotNull(metadatas);
        Assert.assertFalse(metadatas.isEmpty());
        Assert.assertNotNull(metadatas.get(0));
        Assert.assertNotNull(metadatas.get(0).getClassLoader());
    }
}
