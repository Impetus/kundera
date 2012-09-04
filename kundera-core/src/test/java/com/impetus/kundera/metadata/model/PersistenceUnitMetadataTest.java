package com.impetus.kundera.metadata.model;

import java.net.URL;
import java.util.Enumeration;
import java.util.List;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.kundera.loader.PersistenceXMLLoader;

public class PersistenceUnitMetadataTest
{
    private static List<PersistenceUnitMetadata> metadatas;

    @BeforeClass
    public static void setUp() throws Exception
    {
        Enumeration<URL> xmls = Thread.currentThread().getContextClassLoader().getResources("META-INF/persistence.xml");

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
