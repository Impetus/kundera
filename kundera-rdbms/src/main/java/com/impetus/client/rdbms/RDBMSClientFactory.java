/**
 * 
 */
package com.impetus.client.rdbms;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.rdbms.query.RDBMSEntityReader;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.index.LuceneIndexer;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.persistence.EntityReader;

/**
 * A factory for creating RDBMSClient objects.
 * 
 * @author impadmin
 */
public class RDBMSClientFactory extends GenericClientFactory
{

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(RDBMSClientFactory.class);

    /** The index manager. */
    IndexManager indexManager;

    /** The reader. */
    private EntityReader reader;

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.loader.Loader#unload(java.lang.String[])
     */
    @Override
    public void destroy()
    {
        indexManager.close();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.loader.GenericClientFactory#initializeClient()
     */
    @Override
    public void initialize()
    {
        String luceneDirPath = MetadataUtils.getLuceneDirectory(getPersistenceUnit());
        indexManager = new IndexManager(LuceneIndexer.getInstance(new StandardAnalyzer(Version.LUCENE_34),
                luceneDirPath));
        reader = new RDBMSEntityReader();
        ((RDBMSEntityReader) reader).setFilter("where");
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

        // Do nothing.
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.loader.GenericClientFactory#instantiateClient(java
     * .lang.String)
     */
    @Override
    protected Client instantiateClient(String persistenceUnit)
    {
        return new HibernateClient(getPersistenceUnit(), indexManager, reader);
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

}
