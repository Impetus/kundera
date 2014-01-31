package com.impetus.kundera.configure;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.client.ClientResolver;
import com.impetus.kundera.loader.ClientFactory;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.utils.KunderaCoreUtils;

public class ClientMetadataBuilder
{
    /** The log instance. */
    private static Logger log = LoggerFactory.getLogger(ClientMetadataBuilder.class);

    private String[] persistenceUnits;

    private Map mapExternalProperties;

    private SchemaConfiguration schemaConfiguration;

    public ClientMetadataBuilder(Map mapOfPuProperties, final KunderaMetadata kunderaMetadata,
            String... persistenceUnits)
    {
        this.persistenceUnits = persistenceUnits;
        this.mapExternalProperties = mapOfPuProperties;
        this.schemaConfiguration = new SchemaConfiguration(mapOfPuProperties, kunderaMetadata, persistenceUnits);
    }

    public void buildClientFactoryMetadata(Map<String, ClientFactory> clientFactories,
            final KunderaMetadata kunderaMetadata)
    {
        for (String pu : persistenceUnits)
        {
            log.info("Loading client factory for persistence unit " + pu);

            Map<String, Object> puProperty = KunderaCoreUtils.getExternalProperties(pu, mapExternalProperties,
                    persistenceUnits);

            ClientFactory clientFactory = ClientResolver.getClientFactory(pu, puProperty, kunderaMetadata);
            clientFactories.put(pu, clientFactory);
        }

        // configuring schema before loading client factories because during
        // initilazation schema nedds to be created.
        schemaConfiguration.configure();

        // load all client factories.
        for (String pu : persistenceUnits)
        {
            Map<String, Object> puProperty = KunderaCoreUtils.getExternalProperties(pu, mapExternalProperties,
                    persistenceUnits);
            clientFactories.get(pu).load(pu, puProperty);
        }
    }
}
