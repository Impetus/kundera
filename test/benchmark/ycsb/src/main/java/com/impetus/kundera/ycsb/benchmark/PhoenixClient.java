package com.impetus.kundera.ycsb.benchmark;

/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

import com.impetus.kundera.KunderaException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

/**
 * YCSB HBase client for YCSB framework
 */
public class PhoenixClient extends DB
{
    public boolean _debug = false;

    public static String _table = "phoenix_table";

    public static final int Ok = 0;

    public static final int ServerError = -1;

    public static final int HttpError = -2;

    public static final int NoMatchingRecord = -3;

    public static final Object tableLock = new Object();

    public Connection connection/*; //*/ = initConenction();
    
    public Statement stmt;
    
    public PreparedStatement prepstmnt;
    /**
     * Initialize any state for this DB. Called once per DB instance; there is
     * one DB instance per client thread.
     */
    public void init() throws DBException
    {
        try
        {
//            connection = connection!=null?connection:DriverManager.getConnection("jdbc:phoenix");
            stmt = connection.createStatement();
            stmt.executeUpdate("create table if not exists " + _table
                    + "(name varchar not null primary key, age varchar, address varchar, lname varchar)");
        }
        catch (SQLException e)
        {
            System.err.println("Error accessing HBase table: " + e);
        }
    }

    private Connection initConenction()
    {
        try
        {
            return DriverManager.getConnection("jdbc:phoenix");
        }
        catch (SQLException e)
        {
            throw new KunderaException(e);
        }
    }

    /**
     * Read a record from the database. Each field/value pair from the result
     * will be stored in a HashMap.
     * 
     * @param table
     *            The name of the table
     * @param key
     *            The record key of the record to read.
     * @param fields
     *            The list of fields to read, or null for all of them
     * @param result
     *            A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error
     */
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result)
    {

        try
        {
            prepstmnt = connection.prepareStatement("select * from phoenix_table where name = ?");
            prepstmnt.setString(1, key);
            ResultSet rset = prepstmnt.executeQuery();
/*            StringBuilder sb = new StringBuilder("select * from ");
            sb.append(_table);
            sb.append(" where name = ");
            sb.append("'"+key+"'");
            PreparedStatement statement = connection.prepareStatement(sb.toString());
            ResultSet rset = statement.executeQuery();*/
            rset.next();
            assert rset != null;
        }
        catch (SQLException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return Ok;
    }

    /**
     * Perform a range scan for a set of records in the database. Each
     * field/value pair from the result will be stored in a HashMap.
     * 
     * @param table
     *            The name of the table
     * @param startkey
     *            The record key of the first record to read.
     * @param recordcount
     *            The number of records to read
     * @param fields
     *            The list of fields to read, or null for all of them
     * @param result
     *            A Vector of HashMaps, where each HashMap is a set field/value
     *            pairs for one record
     * @return Zero on success, a non-zero error code on error
     */
    public int scan(String table, String startkey, int recordcount, Set<String> fields,
            Vector<HashMap<String, ByteIterator>> result)
    {
        return Ok;
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key, overwriting any existing values with the same field name.
     * 
     * @param table
     *            The name of the table
     * @param key
     *            The record key of the record to write
     * @param values
     *            A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error
     */
    public int update(String table, String key, HashMap<String, ByteIterator> values)
    {
        insert(table, key, values);
        return Ok;
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key.
     * 
     * @param table
     *            The name of the table
     * @param key
     *            The record key of the record to insert.
     * @param values
     *            A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error
     */
    public int insert(String table, String key, HashMap<String, ByteIterator> values)
    {
       
        try{
/*        prepstmnt = connection.prepareStatement("upsert into phoenix_table values (?,?,?,?)");
        prepstmnt.setString(1, key);
        prepstmnt.setString(2, key+"24");
        prepstmnt.setString(3, key+"gzb");
        prepstmnt.setString(4, key+"mishra");*/
        StringBuilder sb = new StringBuilder("upsert into ");
        sb.append(_table);
        sb.append(" values (");
        sb.append("'" + key + "'");
        sb.append(",");
        sb.append("'" + key + "24" + "'");
        sb.append(",");
        sb.append("'" + key + "gzb" + "'");
        sb.append(",");
        sb.append("'" + key + "mishra" + "'");
        sb.append(")");
       /* try
        {*/
//            Statement stmt = connection.createStatement();
            stmt.executeUpdate(sb.toString());
//        assert prepstmnt.executeUpdate()!=0;
            connection.commit();
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
        return Ok;
    }

    /**
     * Delete a record from the database.
     * 
     * @param table
     *            The name of the table
     * @param key
     *            The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error
     */
    public int delete(String table, String key)
    {
        StringBuilder sb = new StringBuilder("delete from ");
        sb.append(_table);
        sb.append(" where  name = ");
        sb.append("'"+key+"'");
        try
        {
//            Statement stmt = connection.createStatement();
            stmt.executeUpdate(sb.toString());
            connection.commit();
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
        return Ok;
    }

    public static void main(String[] args)
    {
        PhoenixClient cli = new PhoenixClient();
        try
        {
            cli.init();
        }
        catch (Exception e)
        {
            System.exit(0);
        }
        long num =200000;
        long i = num;
//        cli.read(_table, "user2679876345976412335", null, null);
        long t1 = System.currentTimeMillis();
        long temp = t1;
        while (i-- != 0)
        {
            cli.insert(_table, ""+i, null);
//            cli.read(null, ""+num, null,null);
            if(i%10000 == 0){
                System.out.println(num-i + " records inserted!");
                System.out.println("time taken = " + (System.currentTimeMillis() - temp));
                temp = System.currentTimeMillis();
            }
            
        }
        System.out.println("time taken = " + (System.currentTimeMillis()-t1));
    }
}

/*
 * For customized vim control set autoindent set si set shiftwidth=4
 */