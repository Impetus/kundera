package com.impetus.client.couchdb;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.configure.AbstractPropertyReader;
import com.impetus.kundera.configure.ClientProperties;
import com.impetus.kundera.configure.PropertyReader;

public class CouchDBPropertyReader extends AbstractPropertyReader implements PropertyReader
{
    /** log instance */
    private static Logger log = LoggerFactory.getLogger(CouchDBPropertyReader.class);

    /** MongoDB schema metadata instance */
    public static CouchDBSchemaMetadata csmd;

    public CouchDBPropertyReader(Map externalProperties)
    {
        super(externalProperties);
        csmd = new CouchDBSchemaMetadata();
    }

    @Override
    protected void onXml(ClientProperties cp)
    {
        if (cp != null)
        {
            csmd.setClientProperties(cp);
        }
    }

    public class CouchDBSchemaMetadata
    {
        private ClientProperties clientProperties;

        public ClientProperties getClientProperties()
        {
            return clientProperties;
        }

        public void setClientProperties(ClientProperties clientProperties)
        {
            this.clientProperties = clientProperties;
        }
    }
}