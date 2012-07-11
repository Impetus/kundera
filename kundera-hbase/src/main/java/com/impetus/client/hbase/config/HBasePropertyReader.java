/**
 * 
 */
package com.impetus.client.hbase.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.KunderaException;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.configure.PropertyReader;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * @author impadmin
 * 
 */
public class HBasePropertyReader implements PropertyReader
{

    private static Log log = LogFactory.getLog(HBasePropertyReader.class);

    public static HBaseSchemaMetadata hsmd;

    public HBasePropertyReader()
    {
        hsmd = new HBaseSchemaMetadata();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.configure.PropertyReader#read(java.lang.String)
     */
    @Override
    public void read(String pu)
    {
        Properties properties = new Properties();
        PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(pu);
        String propertyName = puMetadata != null ? puMetadata
                .getProperty(PersistenceProperties.KUNDERA_CLIENT_PROPERTY) : null;

        InputStream inStream = propertyName != null ? ClassLoader.getSystemResourceAsStream(propertyName) : null;
        if (inStream != null)
        {
            try
            {
                properties.load(inStream);
                readProperties(properties);
            }
            catch (IOException e)
            {
                log.warn("error in loading properties , caused by :" + e.getMessage());
                throw new KunderaException(e);
            }
            finally
            {
                hsmd.setZookeeper_port("2181");
                hsmd.setZookeeper_host("localhost");
            }
        }
        else
        {
            hsmd.setZookeeper_port("2181");
            hsmd.setZookeeper_host("localhost");
            log.warn("No properties found in class path, kundera will use default property");
        }
    }

    private void readProperties(Properties properties)
    {
        String port = properties.getProperty("zookeeper_port");
        hsmd.setZookeeper_port(port);
        String host = properties.getProperty("zookeeper_host");
        hsmd.setZookeeper_host(host);
    }

    public class HBaseSchemaMetadata
    {
        private String zookeeper_port;

        private String zookeeper_host;

        /**
         * @return the zookeeper_port
         */
        public String getZookeeper_port()
        {
            return zookeeper_port;
        }

        /**
         * @param zookeeper_port
         *            the zookeeper_port to set
         */
        public void setZookeeper_port(String zookeeper_port)
        {
            if (zookeeper_port != null)
            {
                this.zookeeper_port = zookeeper_port;
            }
            else
            {
                this.zookeeper_port = "2181";
            }
        }

        /**
         * @return the zookeeper_host
         */
        public String getZookeeper_host()
        {
            return zookeeper_host;
        }

        /**
         * @param zookeeper_host
         *            the zookeeper_host to set
         */
        public void setZookeeper_host(String zookeeper_host)
        {
            if (zookeeper_host != null)
            {
                this.zookeeper_host = zookeeper_host;
            }
            else
            {
                this.zookeeper_host = "localhost";
            }
        }
    }
}
