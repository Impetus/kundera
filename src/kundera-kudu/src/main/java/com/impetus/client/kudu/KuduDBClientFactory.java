/*
 * 
 */
package com.impetus.client.kudu;

import java.util.Map;

import org.kududb.client.KuduClient;

import com.impetus.client.kudu.query.KuduDBEntityReader;
import com.impetus.client.kudu.schemamanager.KuduDBSchemaManager;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * A factory for creating KuduDBClient objects.
 * 
 * @author karthikp.manchala
 */
public class KuduDBClientFactory extends GenericClientFactory
{

    /** The kudu client. */
    private KuduClient kuduClient;

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.loader.ClientFactory#getSchemaManager(java.util.Map)
     */
    @Override
    public SchemaManager getSchemaManager(Map<String, Object> puProperties)
    {
        if (schemaManager == null)
        {
            initializePropertyReader();
            setExternalProperties(puProperties);
            schemaManager = new KuduDBSchemaManager(KuduDBClientFactory.class.getName(), puProperties, kunderaMetadata);
        }
        return schemaManager;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.loader.ClientLifeCycleManager#destroy()
     */
    @Override
    public void destroy()
    {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.loader.GenericClientFactory#initialize(java.util.Map)
     */
    @Override
    public void initialize(Map<String, Object> puProperties)
    {
        reader = new KuduDBEntityReader(kunderaMetadata);
        setExternalProperties(puProperties);
        initializePropertyReader();
        PersistenceUnitMetadata pum = kunderaMetadata.getApplicationMetadata()
                .getPersistenceUnitMetadata(getPersistenceUnit());
        String kuduMasterHost = (String) pum.getProperty("kundera.nodes");
        String kuduMasterPort = (String) pum.getProperty("kundera.port");
        kuduClient = new KuduClient.KuduClientBuilder(kuduMasterHost + ":" + kuduMasterPort).build();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.loader.GenericClientFactory#createPoolOrConnection()
     */
    @Override
    protected Object createPoolOrConnection()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.loader.GenericClientFactory#instantiateClient(java.
     * lang.String)
     */
    @Override
    protected Client instantiateClient(String persistenceUnit)
    {
        return new KuduDBClient(kunderaMetadata, reader, externalProperties, persistenceUnit, this.kuduClient);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.loader.GenericClientFactory#isThreadSafe()
     */
    @Override
    public boolean isThreadSafe()
    {
        // TODO Auto-generated method stub
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.loader.GenericClientFactory#initializeLoadBalancer(
     * java.lang.String)
     */
    @Override
    protected void initializeLoadBalancer(String loadBalancingPolicyName)
    {
        // TODO Auto-generated method stub

    }

    /**
     * Initialize property reader.
     */
    private void initializePropertyReader()
    {
        if (propertyReader == null)
        {
            propertyReader = new KuduDBPropertyReader(externalProperties,
                    kunderaMetadata.getApplicationMetadata().getPersistenceUnitMetadata(getPersistenceUnit()));
            propertyReader.read(getPersistenceUnit());
        }
    }

}
