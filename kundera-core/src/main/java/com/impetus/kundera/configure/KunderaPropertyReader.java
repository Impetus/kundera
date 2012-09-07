package com.impetus.kundera.configure;

import java.io.File;
import java.util.Map;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.thoughtworks.xstream.XStream;

public class KunderaPropertyReader
{
    private static Map<String, KunderaClientProperties> configurationProperties = KunderaMetadata.INSTANCE
            .getApplicationMetadata().getSchemaMetadata().getConfigurationProperties();;

    public void parseXML(String pu)
    {
        PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(pu);
        String propertyName = puMetadata != null ? puMetadata
                .getProperty(PersistenceProperties.KUNDERA_CLIENT_PROPERTY) : null;
        if (propertyName != null)
        {
            XStream stream = new XStream();
            stream.alias("kunderaClientProperties", KunderaClientProperties.class);
            stream.alias("dataStore", KunderaClientProperties.DataStore.class);
            stream.alias("schema", KunderaClientProperties.DataStore.Schema.class);
            stream.alias("table", KunderaClientProperties.DataStore.Schema.Table.class);
            stream.alias("dataCenter", KunderaClientProperties.DataStore.Schema.DataCenter.class);
            stream.alias("connection", KunderaClientProperties.DataStore.Connection.class);
            stream.alias("server", KunderaClientProperties.DataStore.Connection.Server.class);

            Object o = stream.fromXML(new File(propertyName));
            configurationProperties.put(pu, (KunderaClientProperties) o);
        }
    }

    /**
     * @return the configurationProperties
     */
    public static Map<String, KunderaClientProperties> getConfigurationProperties()
    {
        return configurationProperties;
    }
}
