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
import java.util.ArrayList;
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
            String[] persistenceUnits = new String[1];
            persistenceUnits[0] = "kunderatest";
            
            metadatas = PersistenceXMLLoader.findPersistenceUnits(xmls.nextElement(), persistenceUnits);
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
        Assert.assertNotNull(metadatas.get(0).toString());
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
    
    @Test
    public void testAddJarFile()
    {
        Assert.assertNotNull(metadatas);
        Assert.assertFalse(metadatas.isEmpty());
        Assert.assertNotNull(metadatas.get(0));
        PersistenceUnitMetadata puMetadata = metadatas.get(0);
        puMetadata.addJarFile("myJarFile.jar");
        Assert.assertNotNull(puMetadata.getJarFiles());
        Assert.assertFalse(puMetadata.getJarFiles().isEmpty());
        Assert.assertNotNull(puMetadata.getJarFileUrls());
        Assert.assertFalse(puMetadata.getJarFileUrls().isEmpty());        
    }
    
    @Test
    public void testGetFields()
    {
        Assert.assertNotNull(metadatas);
        Assert.assertFalse(metadatas.isEmpty());
        Assert.assertNotNull(metadatas.get(0));
        PersistenceUnitMetadata puMetadata = metadatas.get(0);
        
        Assert.assertFalse(puMetadata.getClasses().isEmpty());
        Assert.assertTrue(puMetadata.getPackages().isEmpty());
        List<String> classes = new ArrayList<String>(); classes.add("MyClass");
        List<String> packages = new ArrayList<String>(); packages.add("com.impetus.my.package");
        
        puMetadata.setClasses(classes);
        puMetadata.setPackages(packages);
        
        Assert.assertFalse(puMetadata.getClasses().isEmpty());
        Assert.assertFalse(puMetadata.getPackages().isEmpty());
        
        Assert.assertNull(puMetadata.getJtaDataSource());
        Assert.assertNull(puMetadata.getNonJtaDataSource());
        Assert.assertNull(puMetadata.getMappingFileNames());
        Assert.assertNull(puMetadata.getSharedCacheMode());
        Assert.assertNull(puMetadata.getValidationMode());
        Assert.assertNull(puMetadata.getNewTempClassLoader());
        Assert.assertNotNull(puMetadata.getMappedUrl());
        Assert.assertNotNull(puMetadata.getProperties());
        Assert.assertTrue(puMetadata.getExcludeUnlistedClasses());
        Assert.assertTrue(puMetadata.excludeUnlistedClasses());
        Assert.assertEquals(0, puMetadata.getBatchSize());     
        
        for(PersistenceUnitMetadata pmd : metadatas)
        {
            try
            {
                pmd.getBatchSize();
            }
            catch (Exception e)
            {                
                Assert.fail(e.getMessage());
             }
            
        }
        
    }  
    
}
