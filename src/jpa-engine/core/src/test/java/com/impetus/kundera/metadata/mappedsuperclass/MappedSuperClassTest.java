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
package com.impetus.kundera.metadata.mappedsuperclass;

import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.KunderaException;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.query.QueryHandlerException;

/**
 * @author vivek.mishra
 * 
 *         MappedSuper class junit.
 * 
 */
public class MappedSuperClassTest
{

    private EntityManagerFactory emf;

    private EntityManager em;

    private String persistenceUnit = "mappedsu";

    @Before
    public void setup()
    {
        emf = Persistence.createEntityManagerFactory("mappedsu");
        em = emf.createEntityManager();
    }

    @Test
    public void testMappedMetamodel()
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(
                ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance(), Employee.class);
        MetamodelImpl metaModel = (MetamodelImpl) ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance()
                .getApplicationMetadata().getMetamodel(persistenceUnit);

        EntityType entityType = metaModel.entity(Employee.class);

        Set<Attribute> attributes = entityType.getAttributes();

        Assert.assertEquals(5, attributes.size());
        Assert.assertNotNull(entityMetadata.getIdAttribute());
        Assert.assertNotNull("id", entityMetadata.getIdAttribute().getName());

    }

    @Test
    public void testValidateQuery()
    {
        String query = " Select p from MappedPerson p";

        try
        {
            em.createQuery(query);
            Assert.fail("Should have gone to catch block!");
        }
        catch (QueryHandlerException qhex)
        {
            Assert.assertEquals("No entity found by the name: MappedPerson", qhex.getMessage());
        }

    }

    @Test
    public void testEntityOperations()
    {

        try
        {
            MappedPerson p = new MappedPerson();
            p.setId("dd");
            p.setFirstName("mapped");
            p.setLastName("superclass");

            em.persist(p);
            Assert.fail("Should have gone to catch block!");
        }
        catch (KunderaException kex)
        {
            Assert.assertNotNull(kex.getMessage());
        }

    }

    @Test
    public void testValidOperations()
    {
        Employee emp = new Employee();
        emp.setId("emp_1");
        emp.setDepartmentId(1);
        emp.setFirstName("vivek");
        em.persist(emp);

        em.clear();
        Employee result = em.find(Employee.class, "emp_1");
        Assert.assertNotNull(result);
        Assert.assertEquals(emp.getFirstName(), result.getFirstName());
        Assert.assertEquals(emp.getDepartmentId(), result.getDepartmentId());

        em.remove(result);

        result = em.find(Employee.class, "emp_1");
        Assert.assertNull(result);
    }

    @After
    public void tearDown()
    {
        if (em != null)
        {
            em.close();
            em = null;
        }

        if (emf != null)
        {
            emf.close();
            emf = null;
        }
    }
}
