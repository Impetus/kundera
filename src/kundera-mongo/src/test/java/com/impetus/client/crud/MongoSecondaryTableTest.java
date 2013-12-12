package com.impetus.client.crud;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MongoSecondaryTableTest extends SecondaryTableTestBase
{

    private EntityManagerFactory emf;

    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory("mongoTest");
    }

    @After
    public void tearDown() throws Exception
    {
        emf.close();
    }

    @Test
    public void test()
    {
        testCRUD(emf);
    }
}
