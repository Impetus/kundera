/**
 * 
 */
package com.impetus.kundera.ycsb.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import com.mongodb.DB;
import com.mongodb.DBAddress;
import com.mongodb.Mongo;
import common.Logger;

/**
 * @author Kuldeep Mishra
 * 
 */
public class MongoDBOperationUtils
{
    private static Logger logger = Logger.getLogger(MongoDBOperationUtils.class);

    /**
     * Stop mongo server.
     * 
     * @param port
     *            the runtime
     * @param br
     *            the br
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     * @throws InterruptedException
     */
    public void cleanDatabase(String url, String dbname) throws IOException, InterruptedException
    {
        logger.info("flushing db ..........");

        if (url.startsWith("mongodb://"))
        {
            url = url.substring(10);
        }

        // need to append db to url.
        url += "/" + dbname;

        Mongo mongo = new Mongo(new DBAddress(url));

        DB db = mongo.getDB(dbname);
        db.dropDatabase();
        mongo.close();
    }

    /**
     * Start Mongo server.
     * 
     * @param runtime
     *            the runtime
     * @return the process
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     * @throws InterruptedException
     */
    public void startMongoServer(Runtime runtime, String startMongoServerCommand) throws IOException, InterruptedException
    {
        logger.info("Starting mongo server at ..........."  + startMongoServerCommand);
        runtime.exec(startMongoServerCommand);
        logger.info("started..............");
	Thread.sleep(90000);
    }

    /**
     * Stop Mongo server.
     * 
     * @param runtime
     *            the runtime
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     * @throws InterruptedException
     */
    public void stopMongoServer(Runtime runtime) throws IOException, InterruptedException
    {
        logger.info("Stoping mongo server..");
        String line;
        Process ps = runtime.exec("ps -ux");
        InputStream is = ps.getInputStream();
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(isr);
        while ((line = br.readLine()) != null)
        {
            StringTokenizer tokenizer = new StringTokenizer(line, " ");
            if (line.contains("mongod"))
            {
                System.out.println(line);
                tokenizer.nextElement();
                String nextElement = (String) tokenizer.nextElement();
                System.out.println(nextElement);
                runtime.exec("kill -9 " + nextElement);
                logger.info("stopped..............");
                break;
            }
        }
    }

    public static void main(String[] args)
    {
        MongoDBOperationUtils utils = new MongoDBOperationUtils();
        Runtime runtime = Runtime.getRuntime();
        String mongoServerLocation = "/home/impadmin/software/mongodb-linux-x86_64-2.0.8/bin";
        try
        {
            utils.startMongoServer(runtime, mongoServerLocation);
        }
        catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (InterruptedException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try
        {
            utils.stopMongoServer(runtime);
        }
        catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (InterruptedException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
