package com.impetus.kundera.client.cassandra.dsdriver;

import java.util.Iterator;
import com.impetus.client.cassandra.CassandraClientBase;
import com.impetus.client.cassandra.query.CassQuery;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.query.KunderaQuery;

/**
 * The Class DSCassQuery.
 * 
 * @author karthikp.manchala
 */
public class DSCassQuery extends CassQuery
{

    /**
     * Instantiates a new DS cass query.
     * 
     * @param kunderaQuery
     *            the kundera query
     * @param persistenceDelegator
     *            the persistence delegator
     * @param kunderaMetadata
     *            the kundera metadata
     */
    public DSCassQuery(KunderaQuery kunderaQuery, PersistenceDelegator persistenceDelegator,
            KunderaMetadata kunderaMetadata)
    {
        super(kunderaQuery, persistenceDelegator, kunderaMetadata);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.client.cassandra.query.CassQuery#iterate()
     */
    @Override
    public Iterator iterate()
    {
        if (kunderaQuery.isNative())
        {
            throw new UnsupportedOperationException("Iteration not supported over native queries");
        }
        EntityMetadata m = getEntityMetadata();
        Client client = persistenceDelegeator.getClient(m);
        externalProperties = ((CassandraClientBase) client).getExternalProperties();

        if (!MetadataUtils.useSecondryIndex(((ClientBase) client).getClientMetadata()))
        {
            throw new UnsupportedOperationException("Scrolling over cassandra is unsupported for lucene queries");
        }

        return new DSResultIterator(this, m, persistenceDelegeator.getClient(m), this.getReader(),
                getFetchSize() != null ? getFetchSize() : this.maxResult, kunderaMetadata);
    }

}
