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
package com.impetus.client.crud;

import java.sql.SQLException;

import javax.persistence.MappedSuperclass;
import javax.persistence.Query;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.client.crud.mappedsuperclass.MappedSuperClassBase;

/**
 * @author vivek.mishra Junit for {@link MappedSuperclass} in RDBMS
 * 
 */
public class RDBMSMappedSuperClassTest extends MappedSuperClassBase
{

    private RDBMSCli cli;

    private static final String SCHEMA = "testdb";

    @Before
    public void setUp() throws Exception
    {
        createSchema();
        _PU = "mappedPu";
        setUpInternal();
    }

    @Test
    public void test() throws Exception
    {
        cli = new RDBMSCli("testdb");
        cli.createSchema("testdb");

        assertInternal(true);
    }

    @After
    public void tearDown() throws Exception
    {
        // // Delete by query.
        String deleteQuery = "Delete from RDBMSCreditTransaction p";

        Query query = em.createQuery(deleteQuery);
        query.executeUpdate();

        deleteQuery = "Delete from RDBMSDebitTransaction p";

        query = em.createQuery(deleteQuery);
        query.executeUpdate();

        tearDownInternal();
        
        dropSchema();

    }

    private void createSchema() throws SQLException
    {
        try
        {
            cli = new RDBMSCli(SCHEMA);
            cli.createSchema(SCHEMA);
            cli.update("CREATE TABLE TESTDB.TRNX_CREDIT (txId VARCHAR(255) PRIMARY KEY, amount int, txStatus VARCHAR(255), tx_type VARCHAR(255), CREDIT_BANK_IDENT VARCHAR(255), transactionDt date)");
            cli.update("CREATE TABLE TESTDB.DebitTransaction (DEBIT_ID VARCHAR(255) PRIMARY KEY, amount int, DEBIT_BANK_IDENT VARCHAR(256), txStatus VARCHAR(255), TX_DT date, tx_type VARCHAR(255))");
        }
        catch (Exception e)
        {

            cli.update("DELETE FROM TESTDB.TRNX_CREDIT");
            cli.update("DELETE FROM TESTDB.ADDRESS");
            cli.update("DROP TABLE TESTDB.TRNX_CREDIT");
            cli.update("DROP TABLE TESTDB.ADDRESS");
            cli.update("DROP SCHEMA TESTDB");
            cli.update("CREATE TABLE TESTDB.TRNX_CREDIT (txId VARCHAR(255) PRIMARY KEY, amount int, txStatus VARCHAR(255), tx_type VARCHAR(255), CREDIT_BANK_IDENT VARCHAR(255), transactionDt date)");
            cli.update("CREATE TABLE TESTDB.DebitTransaction (DEBIT_ID VARCHAR(255) PRIMARY KEY, amount int, DEBIT_BANK_IDENT VARCHAR(256), txStatus VARCHAR(255), TX_DT date, tx_type VARCHAR(255))");
            // nothing
            // do
        }
    }

    private void dropSchema()
    {
        try
        {
            cli.update("DELETE FROM TESTDB.TRNX_CREDIT");
            cli.update("DELETE FROM TESTDB.DebitTransaction");
            cli.update("DROP TABLE TESTDB.TRNX_CREDIT");
            cli.update("DROP TABLE TESTDB.DebitTransaction");
            cli.update("DROP SCHEMA TESTDB");
            cli.closeConnection();
        }
        catch (Exception e)
        {
            // Nothing to do
        }
    }
}
