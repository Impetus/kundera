package com.impetus.client.hbase;

import com.impetus.kundera.loader.Loader;
import com.impetus.kundera.metadata.model.ClientMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;

public class HBaseClientLoader implements Loader
{

    @Override
    public void load(String persistenceUnit)
    {	
    	
    	//Load client related metadata
        loadClientMetadata(persistenceUnit);

    }
    
    private void loadClientMetadata(String persistenceUnit)
    {
        ClientMetadata clientMetadata = new ClientMetadata();

        // TODO Make a client properties file
        clientMetadata.setClientImplementor("com.impetus.client.hbase.HBaseClient");
        clientMetadata.setIndexImplementor("com.impetus.kundera.index.LuceneIndexer");
        
        if (KunderaMetadata.getInstance().getClientMetadata(persistenceUnit) == null)
            KunderaMetadata.getInstance().addClientMetadata(persistenceUnit, clientMetadata);
    }

}
