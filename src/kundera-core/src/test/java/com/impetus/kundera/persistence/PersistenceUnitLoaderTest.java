/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
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
package com.impetus.kundera.persistence;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.List;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.loader.PersistenceXMLLoader;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * Unit test case to load persistence.xml properties.
 * 
 * @author vivek.mishra
 * 
 */
public class PersistenceUnitLoaderTest
{
    /** logger instance */
    private static final Logger log = LoggerFactory.getLogger(PersistenceUnitLoaderTest.class);

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        // do nothing.
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        // do nothing.
    }

    /**
     * Test method on persistence unit loading.
     */
    @Test
    public void testLoadPersistenceUnitLoading()
    {
        try
        {
            Enumeration<URL> xmls = this.getClass().getClassLoader()
                    .getResources("META-INF/persistence.xml");

            while (xmls.hasMoreElements())
            {
                final String _pattern = "/kundera-core/target/test-classes/";
                List<PersistenceUnitMetadata> metadatas = PersistenceXMLLoader.findPersistenceUnits(xmls.nextElement());
                Assert.assertNotNull(metadatas);
                Assert.assertEquals(10, metadatas.size());

                // commented out to keep ConfiguratorTest happy! as it tries to
                // load it.
                // Assert.assertEquals(2,
                // metadatas.get(0).getJarFiles().size());

                Assert.assertEquals(52, metadatas.get(0).getClasses().size());
                Assert.assertNotNull(metadatas.get(0).getPersistenceUnitRootUrl());
                Assert.assertTrue(metadatas.get(0).getPersistenceUnitRootUrl().getPath().endsWith(_pattern));
            }
        }
        catch (IOException ioex)
        {
            log.error(ioex.getMessage());
            Assert.fail();
        }
        catch (Exception e)
        {
            log.error(e.getMessage());
            Assert.fail();
        }

    }
}
