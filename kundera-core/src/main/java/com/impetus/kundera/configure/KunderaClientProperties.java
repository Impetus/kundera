package com.impetus.kundera.configure;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * 
 * @author Kuldeep Mishra
 * 
 */
@XmlRootElement
public class KunderaClientProperties implements Serializable
{
    private static final long serialVersionUID = 1L;

    private List<DataStore> datastores;

    /**
     * @return the datastores
     */
    @XmlElement
    public List<DataStore> getDatastores()
    {
        return datastores;
    }

    /**
     * @param datastores
     *            the datastores to set
     */
    public void setDatastores(List<DataStore> datastores)
    {
        this.datastores = datastores;
    }

    @XmlRootElement
    public static class DataStore
    {
        private Connection connection;

        private List<Schema> schemas;

        private String name;

        /**
         * @return the connection
         */
        @XmlElement
        public Connection getConnection()
        {
            return connection;
        }

        /**
         * @param connection
         *            the connection to set
         */
        public void setConnection(Connection connection)
        {
            this.connection = connection;
        }

        /**
         * @return the schemas
         */
        @XmlElement
        public List<Schema> getSchemas()
        {
            return schemas;
        }

        /**
         * @return the name
         */
        public String getName()
        {
            return name;
        }

        /**
         * @param name
         *            the name to set
         */
        public void setName(String name)
        {
            this.name = name;
        }

        /**
         * @param schemas
         *            the schemas to set
         */
        public void setSchemas(List<Schema> schemas)
        {
            this.schemas = schemas;
        }

        @XmlRootElement
        public static class Schema
        {
            private List<Table> tables;

            // private List<Properties> schemaProperties;
            private Properties properties;

            private List<DataCenter> dataCenters;

            private String name;

            /**
             * @return the name
             */
            public String getName()
            {
                return name;
            }

            /**
             * @param name
             *            the name to set
             */
            public void setName(String name)
            {
                this.name = name;
            }

            /**
             * @return the tables
             */
            public List<Table> getTables()
            {
                return tables;
            }

            /**
             * @param tables
             *            the tables to set
             */
            public void setTables(List<Table> tables)
            {
                this.tables = tables;
            }

            /**
             * @return the schemaProperties
             */
            public Properties getSchemaProperties()
            {
                return properties;
            }

            /**
             * @param schemaProperties
             *            the schemaProperties to set
             */
            public void setProperties(Properties props)
            {
                this.properties = props;
            }

            /**
             * @return the dataCenters
             */
            public List<DataCenter> getDataCenters()
            {
                return dataCenters;
            }

            /**
             * @param dataCenters
             *            the dataCenters to set
             */
            public void setDataCenters(List<DataCenter> dataCenters)
            {
                this.dataCenters = dataCenters;
            }

            public static class Table
            {
                private Properties properties;

                private String name;

                /**
                 * @return the name
                 */
                public String getName()
                {
                    return name;
                }

                /**
                 * @param name
                 *            the name to set
                 */
                public void setName(String name)
                {
                    this.name = name;
                }

                /**
                 * @return the properties
                 */
                public Properties getProperties()
                {
                    return properties;
                }

                /**
                 * @param properties
                 *            the properties to set
                 */
                public void setProperties(Properties properties)
                {
                    this.properties = properties;
                }

            }

            public static class DataCenter
            {
                private String name;

                private String value;

                /**
                 * @return the name
                 */
                public String getName()
                {
                    return name;
                }

                /**
                 * @param name
                 *            the name to set
                 */
                public void setName(String name)
                {
                    this.name = name;
                }

                /**
                 * @return the value
                 */
                public String getValue()
                {
                    return value;
                }

                /**
                 * @param value
                 *            the value to set
                 */
                public void setValue(String value)
                {
                    this.value = value;
                }
            }
        }

        public static class Connection
        {
            private Properties properties;

            private List<Server> servers;

            /**
             * @return the properties
             */
            public Properties getProperties()
            {
                return properties;
            }

            /**
             * @param properties
             *            the properties to set
             */
            public void setProperties(Properties properties)
            {
                this.properties = properties;
            }

            /**
             * @return the servers
             */
            public List<Server> getServers()
            {
                return servers;
            }

            /**
             * @param servers
             *            the servers to set
             */
            public void setServers(List<Server> servers)
            {
                this.servers = servers;
            }

            public static class Server
            {
                private String host;

                private String port;

                /**
                 * @return the host
                 */
                public String getHost()
                {
                    return host;
                }

                /**
                 * @param host
                 *            the host to set
                 */
                public void setHost(String host)
                {
                    this.host = host;
                }

                /**
                 * @return the port
                 */
                public String getPort()
                {
                    return port;
                }

                /**
                 * @param port
                 *            the port to set
                 */
                public void setPort(String port)
                {
                    this.port = port;
                }
            }
        }
    }
}
