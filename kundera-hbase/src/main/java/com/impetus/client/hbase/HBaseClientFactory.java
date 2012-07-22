/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/
package com.impetus.client.hbase;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;

import com.impetus.client.hbase.config.HBasePropertyReader;
import com.impetus.client.hbase.schemamanager.HBaseSchemaManager;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.configure.PropertyReader;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.index.LuceneIndexer;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.persistence.EntityReader;

/**
 * HBaseClientFactory, instantiates client for HBase
 */
public class HBaseClientFactory extends GenericClientFactory
{

    /** The index manager. */
    private IndexManager indexManager;

    /** The conf. */
    private HBaseConfiguration conf;

    /** The h table pool. */
    private HTablePool hTablePool;

    /** The reader. */
    private EntityReader reader;

    /** The Constant DEFAULT_POOL_SIZE. */
    private static final int DEFAULT_POOL_SIZE = 100;

    /** The pool size. */
    private int poolSize;

    /** Configure schema manager. */
    private SchemaManager schemaManager;

    /** property reader instance */
    private PropertyReader propertyReader;

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.loader.GenericClientFactory#initializeClient()
     */
    @Override
    public void initialize()
    {
        // Initialize Index Manager
        String luceneDirPath = MetadataUtils.getLuceneDirectory(getPersistenceUnit());
        indexManager = new IndexManager(LuceneIndexer.getInstance(new StandardAnalyzer(Version.LUCENE_34),
                luceneDirPath));

        // Initialize HBase configuration
        PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(getPersistenceUnit());

        String node = puMetadata.getProperties().getProperty(PersistenceProperties.KUNDERA_NODES);
        String port = puMetadata.getProperties().getProperty(PersistenceProperties.KUNDERA_PORT);
        String poolSize = puMetadata.getProperties().getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_ACTIVE);

        if (StringUtils.isEmpty(poolSize))
        {
            this.poolSize = DEFAULT_POOL_SIZE;
        }
        else
        {
            this.poolSize = Integer.parseInt(poolSize);
        }

        propertyReader = new HBasePropertyReader();
        propertyReader.read(getPersistenceUnit());

        Configuration hadoopConf = new Configuration();
        hadoopConf.set("hbase.master", node + ":" + port);
        hadoopConf.set("hbase.zookeeper.quorum", HBasePropertyReader.hsmd.getZookeeperHost());
        hadoopConf.set("hbase.zookeeper.property.clientPort", HBasePropertyReader.hsmd.getZookeeperPort());
        conf = new HBaseConfiguration(hadoopConf);
        reader = new HBaseEntityReader();
        //
        // schemaManager = new
        // HBaseSchemaManager(HBaseClientFactory.class.getName());
        // schemaManager.exportSchema();
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
        hTablePool = new HTablePool(conf, poolSize);
        return hTablePool;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.loader.GenericClientFactory#instantiateClient()
     */
    @Override
    protected Client instantiateClient(String persistenceUnit)
    {
        return new HBaseClient(indexManager, conf, hTablePool, reader, persistenceUnit);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.loader.GenericClientFactory#isClientThreadSafe()
     */
    @Override
    public boolean isThreadSafe()
    {
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.loader.Loader#unload(java.lang.String[])
     */
    @Override
    public void destroy()
    {
        // TODO destroy pool
        // hTablePool = null;

        indexManager.close();
        getSchemaManager().dropSchema();
    }

    @Override
    public SchemaManager getSchemaManager()
    {
        if (schemaManager == null)
        {
            schemaManager = new HBaseSchemaManager(HBaseClientFactory.class.getName());
        }
        return schemaManager;
    }
}
