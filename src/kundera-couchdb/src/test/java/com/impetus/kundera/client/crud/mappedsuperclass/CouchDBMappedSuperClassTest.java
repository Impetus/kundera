package com.impetus.kundera.client.crud.mappedsuperclass;

import javax.persistence.Query;

import org.apache.http.HttpHost;
import org.apache.http.client.HttpClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.couchdb.utils.CouchDBTestUtils;

public class CouchDBMappedSuperClassTest extends MappedSuperClassBase
{

    private HttpClient httpClient;

    private HttpHost httpHost;

    @Before
    public void setUp() throws Exception
    {
        _PU = "couchdb_pu";
        setUpInternal();
        httpClient = CouchDBTestUtils.initiateHttpClient(_PU);
        httpHost = new HttpHost("localhost", 5984);
        CouchDBTestUtils.createViews(new String[] { "CREDIT_BANK_IDENT" }, "TRNX_CREDIT", httpHost, "couchdatabase",
                httpClient);
        CouchDBTestUtils.createViews(new String[] { "DEBIT_BANK_IDENT" }, "DebitTransaction", httpHost, "couchdatabase",
                httpClient);
    }

    @Test
    public void test()
    {
        assertInternal(true);
    }

    @After
    public void tearDown() throws Exception
    {
        // Delete by query.
        String deleteQuery = "Delete from CreditTransaction p";

        Query query = em.createQuery(deleteQuery);
        query.executeUpdate();

        deleteQuery = "Delete from DebitTransaction p";

        query = em.createQuery(deleteQuery);
        query.executeUpdate();

        tearDownInternal();
        CouchDBTestUtils.dropDatabase("couchdatabase", httpClient, httpHost);
    }
}
