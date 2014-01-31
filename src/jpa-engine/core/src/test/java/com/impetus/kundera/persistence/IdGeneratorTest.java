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
package com.impetus.kundera.persistence;

import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.CoreTestUtilities;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.CoreTestClientNoGenerator;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.validator.GeneratedIdStrategyAuto;
import com.impetus.kundera.metadata.validator.GeneratedIdStrategyIdentity;
import com.impetus.kundera.metadata.validator.GeneratedIdStrategySequence;
import com.impetus.kundera.metadata.validator.GeneratedIdStrategyTable;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.thoughtworks.xstream.core.ReferenceByIdMarshaller.IDGenerator;

/**
 * @author vivek.mishra
 * 
 *         junit for {@link IDGenerator}
 * 
 */
public class IdGeneratorTest
{
    private static String persistenceUnit = "GeneratedValue";

    private EntityManagerFactory emf;

    private EntityManager em;

    private KunderaMetadata kunderaMetadata;

    @Before
    public void setup()
    {
        emf = Persistence.createEntityManagerFactory(persistenceUnit);
        kunderaMetadata = ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance();
        em = emf.createEntityManager();
    }

    @Test
    public void testAutoStrategy() throws NoSuchFieldException, SecurityException, IllegalArgumentException,
            IllegalAccessException
    {
        IdGenerator idGenerator = new IdGenerator();

        GeneratedIdStrategyAuto autoStrategy = new GeneratedIdStrategyAuto();
        autoStrategy.setName("auto strategy");

        Assert.assertEquals(0, autoStrategy.getId());

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata,
                GeneratedIdStrategyAuto.class);

        // on auto strategy
        idGenerator.generateAndSetId(autoStrategy, entityMetadata, CoreTestUtilities.getDelegator(em), kunderaMetadata);
        Assert.assertTrue(autoStrategy.getId() > 0);
    }

    @Test
    public void testSequenceStrategy() throws NoSuchFieldException, SecurityException, IllegalArgumentException,
            IllegalAccessException
    {
        IdGenerator idGenerator = new IdGenerator();

        GeneratedIdStrategySequence seqStrategy = new GeneratedIdStrategySequence();
        seqStrategy.setName("sequence strategy");

        Assert.assertEquals(0, seqStrategy.getId());

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata,
                GeneratedIdStrategySequence.class);

        // on auto strategy
        idGenerator.generateAndSetId(seqStrategy, entityMetadata, CoreTestUtilities.getDelegator(em), kunderaMetadata);
        Assert.assertTrue(seqStrategy.getId() > 0);

        try
        {
            setInvalidClient(CoreTestUtilities.getDelegator(em));
            idGenerator.generateAndSetId(seqStrategy, entityMetadata, CoreTestUtilities.getDelegator(em),
                    kunderaMetadata);
            Assert.fail("Should have gone to catch block!");
        }
        catch (IllegalArgumentException iaex)
        {
            Assert.assertNotNull(iaex);
        }
    }

    @Test
    public void testIdentityStrategy() throws NoSuchFieldException, SecurityException, IllegalArgumentException,
            IllegalAccessException
    {
        IdGenerator idGenerator = new IdGenerator();

        GeneratedIdStrategyIdentity identityStrategy = new GeneratedIdStrategyIdentity();
        identityStrategy.setName("identity strategy");

        Assert.assertEquals(0, identityStrategy.getId());

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata,
                GeneratedIdStrategyIdentity.class);

        // on auto strategy

        try
        {
            idGenerator.generateAndSetId(identityStrategy, entityMetadata, CoreTestUtilities.getDelegator(em),
                    kunderaMetadata);
        }
        catch (UnsupportedOperationException usex)
        {
            Assert.assertNotNull(usex);
        }
    }

    @Test
    public void testTableStrategy() throws NoSuchFieldException, SecurityException, IllegalArgumentException,
            IllegalAccessException
    {
        IdGenerator idGenerator = new IdGenerator();

        GeneratedIdStrategyTable tableStrategy = new GeneratedIdStrategyTable();
        tableStrategy.setName("table strategy");

        Assert.assertEquals(0, tableStrategy.getId());

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata,
                GeneratedIdStrategyTable.class);

        // on auto strategy
        idGenerator
                .generateAndSetId(tableStrategy, entityMetadata, CoreTestUtilities.getDelegator(em), kunderaMetadata);
        Assert.assertTrue(tableStrategy.getId() > 0);

        try
        {
            setInvalidClient(CoreTestUtilities.getDelegator(em));
            idGenerator.generateAndSetId(tableStrategy, entityMetadata, CoreTestUtilities.getDelegator(em),
                    kunderaMetadata);
            Assert.fail("Should have gone to catch block!");
        }
        catch (IllegalArgumentException iaex)
        {
            Assert.assertNotNull(iaex);
        }
    }

    private void setInvalidClient(PersistenceDelegator pd) throws NoSuchFieldException, SecurityException
    {
        Map<String, Client> clientMap = new HashMap<String, Client>();
        clientMap.put(persistenceUnit, new CoreTestClientNoGenerator(null, persistenceUnit, kunderaMetadata));
        PropertyAccessorHelper.set(pd, pd.getClass().getDeclaredField("clientMap"), clientMap);
    }

    @After
    public void tearDown()
    {
        if (em != null)
            em.close();

        if (emf != null)
            emf.close();
    }
}
