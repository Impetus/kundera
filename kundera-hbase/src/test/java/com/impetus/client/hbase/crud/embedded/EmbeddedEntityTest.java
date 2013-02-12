package com.impetus.client.hbase.crud.embedded;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;
import javax.servlet.UnavailableException;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.hbase.junits.HBaseCli;
import com.impetus.kundera.metadata.model.KunderaMetadata;

public class EmbeddedEntityTest
{

    /**
     * 
     */
    private static final Date CAPTURE_TIME = new Date();

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;

    private static HBaseCli cli;

    @BeforeClass
    public static void setUpBeforeClass() throws IOException
    {
        cli = new HBaseCli();
        cli.startCluster();
        KunderaMetadata.INSTANCE.setApplicationMetadata(null);
        emf = Persistence.createEntityManagerFactory("hbaseTest");
    }

    @Before
    public void setUp() throws IOException, TException, UnavailableException
    {
        em = emf.createEntityManager();
    }

    @Test
    public void testSelectEmbeddedColumn()
    {
        persist();
        String query2 = "select x.id," + "x.rowKey," + "x.established, " + "x.closeWait," + "x.finWait,"
                + "x.finWait2," + "x.idle," + "x.listen," + "x.synRecv," + "x.timeWait," + "x.total "
                + "from NetstatData x " + "where x.id.server= :serverid";

        Query query = em.createQuery(query2);
        query.setParameter("serverid", "apdwa570");
        List<NetstatData> result = query.getResultList();
        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.size());
        Assert.assertNotNull(result.get(0));
        Assert.assertNotNull(result.get(0).getId());
        Assert.assertEquals(CAPTURE_TIME, result.get(0).getId().getCaptureTime());
        Assert.assertEquals("apdwa570", result.get(0).getId().getServer());
        Assert.assertNull(result.get(0).getId().getPortMapId());
    }

    @Test
    public void testSelectOnlySpecificColumnOfEmbeddable()
    {
        persist();
        String query2 = "select x.id.captureTime," + "x.rowKey," + "x.established, " + "x.closeWait," + "x.finWait,"
                + "x.finWait2," + "x.idle," + "x.listen," + "x.synRecv," + "x.timeWait," + "x.total "
                + "from NetstatData x " + "where x.id.server= :serverid";

        Query query = em.createQuery(query2);
        query.setParameter("serverid", "apdwa570");
        List<NetstatData> result = query.getResultList();
        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.size());
        Assert.assertNotNull(result.get(0));
        Assert.assertNotNull(result.get(0).getId());
        Assert.assertEquals(CAPTURE_TIME, result.get(0).getId().getCaptureTime());
        Assert.assertNull(result.get(0).getId().getServer());
        Assert.assertNull(result.get(0).getId().getPortMapId());
    }

    @Test
    public void testSelectTwoSpecificColumnOfEmbeddable()
    {
        persist();
        String query2 = "select x.id.captureTime," + "x.id.server," + "x.rowKey," + "x.established, " + "x.closeWait,"
                + "x.finWait," + "x.finWait2," + "x.idle," + "x.listen," + "x.synRecv," + "x.timeWait," + "x.total "
                + "from NetstatData x " + "where x.id.server= :serverid";

        Query query = em.createQuery(query2);
        query.setParameter("serverid", "apdwa570");
        List<NetstatData> result = query.getResultList();
        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.size());
        Assert.assertNotNull(result.get(0));
        Assert.assertNotNull(result.get(0).getId());
        Assert.assertEquals(CAPTURE_TIME, result.get(0).getId().getCaptureTime());
        Assert.assertEquals("apdwa570", result.get(0).getId().getServer());
        Assert.assertNull(result.get(0).getId().getPortMapId());
    }

    @Test
    public void testSelectAll()
    {
        persist();
        String query2 = "select x from NetstatData x " + "where x.id.server= :serverid";

        Query query = em.createQuery(query2);
        query.setParameter("serverid", "apdwa570");
        List<NetstatData> result = query.getResultList();
        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.size());
        Assert.assertNotNull(result.get(0));
        Assert.assertNotNull(result.get(0).getId());
        Assert.assertEquals(CAPTURE_TIME, result.get(0).getId().getCaptureTime());
        Assert.assertEquals("apdwa570", result.get(0).getId().getServer());
        Assert.assertNull(result.get(0).getId().getPortMapId());
    }

    @After
    public void tearDown()
    {
        em.close();

    }

    @AfterClass
    public static void tearDownAfterClass()
    {
        emf.close();
        if (cli != null && cli.isStarted())
        {
            cli.dropTable("NETSTAT_DTL_SMRY");
        }
    }

    /**
     * 
     */
    private void persist()
    {
        NetstatData data = new NetstatData();
        NetstatDataId id = new NetstatDataId();
        id.setServer("apdwa570");
        data.setRowKey("rowKey1");
        id.setCaptureTime(CAPTURE_TIME);
        id.setPortMapTypeCd("portMapTypeCd");
        data.setEstablished(1);
        data.setTotal(1);
        data.setId(id);

        em.persist(data);

        em.clear();
    }
}
