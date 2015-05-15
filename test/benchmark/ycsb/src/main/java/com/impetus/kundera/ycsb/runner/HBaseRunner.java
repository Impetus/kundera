/**
 * 
 */
package com.impetus.kundera.ycsb.runner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.Configuration;

import com.impetus.kundera.ycsb.utils.HBaseOperationUtils;
import com.impetus.kundera.ycsb.utils.MailUtils;

/**
 * @author vivek.mishra
 * 
 */
public class HBaseRunner extends YCSBRunner
{
    private String startHBaseServerCommand;

    private String stopHBaseServerCommand;

    public HBaseRunner(String propertyFile, Configuration config)
    {
        super(propertyFile, config);
        String server = config.getString("server.location");
        this.startHBaseServerCommand = server+"start-hbase.sh";
        this.stopHBaseServerCommand = "/root/software/stopServers.sh";
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.ycsb.runner.YCSBRunner#startServer(boolean,
     * java.lang.Runtime)
     */
    @Override
    public void startServer(boolean performDelete, Runtime runTime)
    {
        if (performDelete)
        {
            try
            {
                HBaseOperationUtils.startHBaseServer(runTime, startHBaseServerCommand);
                HBaseOperationUtils utils = new HBaseOperationUtils();
//                utils.createTable(schema, columnFamilyOrTable);
                utils.deleteAllTables();
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

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.ycsb.runner.YCSBRunner#stopServer(java.lang.Runtime)
     */
    @Override
    public void stopServer(Runtime runTime)
    {
        try
        {
            HBaseOperationUtils.stopHBaseServer(stopHBaseServerCommand, runTime);
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

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.ycsb.runner.YCSBRunner#sendMail()
     */
    @Override
    protected void sendMail()
    {
        Map<String, Double> delta = new HashMap<String, Double>();

        delta.put("throughput of " + clients[0] + " = ", timeTakenByClient.get(clients[0]).doubleValue());
        delta.put("throughput of " + clients[1] + " = ", timeTakenByClient.get(clients[1]).doubleValue());
        double kunderaHBaseToPhoenixDelta = ((timeTakenByClient.get(clients[1]).doubleValue() - timeTakenByClient.get(
                clients[0]).doubleValue())
                / timeTakenByClient.get(clients[1]).doubleValue() * 100);
        delta.put("kunderaHBaseToPhoenixDelta ==> ", kunderaHBaseToPhoenixDelta);

        if (kunderaHBaseToPhoenixDelta > 8.00)
        {
            MailUtils.sendMail(delta, isUpdate ? "update" : runType, "hbase");
        } else
        {
            MailUtils.sendPositiveEmail(delta, isUpdate ? "update" : runType, "hbase");
            
        }
    }

}
