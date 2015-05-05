package com.impetus.kundera.ycsb.utils;

import java.io.IOException;

import javax.persistence.PersistenceException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import common.Logger;

public final class HBaseOperationUtils
{

    private Admin admin;

    private static Logger logger = Logger.getLogger(HBaseOperationUtils.class);

    public HBaseOperationUtils()
    {
        try
        {
            Connection conn  = ConnectionFactory.createConnection();
            admin = conn.getAdmin();
        }
        catch (Exception e)
        {
            throw new PersistenceException(e);
        }
    }

    public void deleteTable(String name) throws IOException
    {
        admin.disableTable(TableName.valueOf(name));
        admin.deleteTable(TableName.valueOf(name));
        
    }


    public void createTable(String name, String familyName) throws IOException
    {
    	String tableName = name + ":" + familyName;
        if(admin.tableExists(TableName.valueOf(tableName)))
        {
            deleteTable(tableName);
            admin.deleteNamespace(name);
        }
        NamespaceDescriptor descriptor = NamespaceDescriptor.create(name).build();
        admin.createNamespace(descriptor);
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor columnFamily = new HColumnDescriptor(familyName);
        table.addFamily(columnFamily);
        admin.createTable(table);
    }


    public void deleteAllTables()
    {
        try
        {
            admin.disableTables(".*");
            admin.deleteTables(".*");
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

    }
    /**
     * Start HBase server.
     * 
     * @param runtime
     *            the runtime
     * @return the process
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     * @throws InterruptedException
     */
    public static void startHBaseServer(Runtime runtime, String startHBaseServerCommand) throws IOException,
            InterruptedException
    {
        logger.info("Starting hbase server ...........");
        runtime.exec(startHBaseServerCommand);
        Thread.sleep(40000);
        logger.info("started..............");
    }

    /**
     * Stop HBase server.
     * 
     * @param runtime
     *            the runtime
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     * @throws InterruptedException
     */
    public static void stopHBaseServer(String stopHBaseServerCommand, Runtime runtime) throws IOException,
            InterruptedException
    {
        logger.info("Stoping hbase server..");
        runtime.exec(stopHBaseServerCommand);
        Thread.sleep(60000);
        logger.info("stopped..............");
    }

    public static void main(String[] args)
    {
//        HBaseOperationUtils utils = new HBaseOperationUtils();
        Runtime runtime = Runtime.getRuntime();
        String startHBaseServercommand = "/home/impadmin/software/hbase-0.94.3/bin/start-hbase.sh";
        String stopHBaseServercommand = "/home/impadmin/software/hbase-0.94.3/bin/stop-hbase.sh";
        try
        {
            HBaseOperationUtils.startHBaseServer(runtime, startHBaseServercommand);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        try
        {
            HBaseOperationUtils.stopHBaseServer(stopHBaseServercommand, runtime);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }
}
