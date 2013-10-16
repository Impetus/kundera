package com.impetus.client.couchdb;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.client.Client;

public class CouchDbDBClientProperties
{
    /** log for this class. */
    private static Logger log = LoggerFactory.getLogger(CouchDbDBClientProperties.class);

    public static final String BATCH_SIZE = "batch.size";

    private CouchDBClient couchDBClient;

    public void populateClientProperties(Client client, Map<String, Object> properties)
    {
        this.couchDBClient = (CouchDBClient) client;

        if (properties != null)
        {
            for (String key : properties.keySet())
            {
                Object value = properties.get(key);
                if (checkNull(key, value))
                {
                    if (key.equals(BATCH_SIZE))
                    {
                        setBatchSize(value);

                    }
                }
                // Add more properties as needed
            }
        }
    }

    /**
     * set batch size
     */
    private void setBatchSize(Object value)
    {
        if (value instanceof Integer)
        {
            this.couchDBClient.setBatchSize((Integer) value);

        }
        else if (value instanceof String)
        {

            this.couchDBClient.setBatchSize(Integer.valueOf((String) value));
        }
    }

    /**
     * check key value map not null
     */
    private boolean checkNull(String key, Object value)
    {
        return key != null && value != null;
    }
}