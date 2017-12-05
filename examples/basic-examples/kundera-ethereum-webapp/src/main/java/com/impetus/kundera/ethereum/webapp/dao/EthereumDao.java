package com.impetus.kundera.ethereum.webapp.dao;

import java.math.BigInteger;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.springframework.stereotype.Service;

import com.impetus.kundera.blockchain.ethereum.BlockchainImporter;

@Service
public class EthereumDao
{

    private static BlockchainImporter importer = BlockchainImporter.initialize();

    public void importBlocks(long from, long to)
    {
        importer.importBlocks(BigInteger.valueOf(from), BigInteger.valueOf(to));
    }

    public List runJPAQuery(String query)
    {
        EntityManager em = BlockchainImporter.getKunderaWeb3jClient().getEntityManager();
        Query q = (Query) em.createQuery(query);
        return q.getResultList();
    }

}
