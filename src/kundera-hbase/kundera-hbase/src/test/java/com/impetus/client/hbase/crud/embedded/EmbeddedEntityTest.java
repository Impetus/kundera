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

public class EmbeddedEntityTest
{

    private static final Date CAPTURE_TIME = new Date();

    private static final Date CAPTURE_TIME1 = new Date("Feb 14 10:01:25 MST 2010");

    private static final Date CAPTURE_TIME2 = new Date(Long.MAX_VALUE);

    private static final Date CAPTURE_TIME3 = new Date("Feb 14 10:10:17 MST 2016");

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
        String query2 = "select x.id," + "x.rowKey," + "x.established, " + "x.closeWait," + "x.finWait," + "x.finWait2,"
                + "x.idle," + "x.listen," + "x.synRecv," + "x.timeWait," + "x.total " + "from NetstatData x "
                + "where x.id.server= :serverid";

        Query query = em.createQuery(query2);
        query.setParameter("serverid", "apdwa570");
        List<NetstatData> result = query.getResultList();
        Assert.assertNotNull(result);
        Assert.assertEquals(2, result.size());
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
        String query2 = "select x.id.server, x.id.captureTime," + "x.rowKey," + "x.established, " + "x.closeWait,"
                + "x.finWait," + "x.finWait2," + "x.idle," + "x.listen," + "x.synRecv," + "x.timeWait," + "x.total "
                + "from NetstatData x " + "where x.id.server= :serverid";

        Query query = em.createQuery(query2);
        query.setParameter("serverid", "apdwa571");
        List<NetstatData> result = query.getResultList();
        Assert.assertNotNull(result);
        Assert.assertEquals(2, result.size());
        Assert.assertNotNull(result.get(0));
        Assert.assertNotNull(result.get(0).getId());
        Assert.assertEquals(CAPTURE_TIME2, result.get(0).getId().getCaptureTime());
        Assert.assertNotNull(result.get(0).getId().getServer());
        Assert.assertNull(result.get(0).getId().getPortMapId());
    }

    @Test
    public void testSelectTwoSpecificColumnOfEmbeddable()
    {
        persist();
        String query2 = "select x.id.captureTime," + "x.id.server," + "x.rowKey," + "x.established, " + "x.closeWait,"
                + "x.finWait," + "x.finWait2," + "x.idle," + "x.listen," + "x.synRecv," + "x.timeWait," + "x.total "
                + "from NetstatData x " + "where x.id.portMapTypeCd= :port"
                + " and x.id.captureTime between :captureTime1 and :captureTime2";

        Query query = em.createQuery(query2);
        query.setParameter("port", "portMapTypeCd");
        query.setParameter("captureTime1", CAPTURE_TIME1);
        query.setParameter("captureTime2", CAPTURE_TIME3);

        List<NetstatData> result = query.getResultList();
        Assert.assertNotNull(result);
        Assert.assertEquals(2, result.size());
        Assert.assertNotNull(result.get(0));
        Assert.assertNotNull(result.get(0).getId());
        Assert.assertNull(result.get(0).getId().getPortMapId());
    }

    @Test
    public void testSelectTwoColumnsOfEmbeddable()
    {
        persist();
        String query2 = "select x.id.captureTime," + "x.id.server," + "x.rowKey," + "x.established, " + "x.closeWait,"
                + "x.finWait," + "x.finWait2," + "x.idle," + "x.listen," + "x.synRecv," + "x.timeWait," + "x.total "
                + "from NetstatData x " + "where x.id.server= :serverId"
                + " and x.id.captureTime between :captureTime1 and :captureTime2";

        Query query = em.createQuery(query2);
        query.setParameter("serverId", "apdwa571");
        query.setParameter("captureTime1", CAPTURE_TIME1);
        query.setParameter("captureTime2", CAPTURE_TIME3);

        List<NetstatData> result = query.getResultList();
        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.size());
        Assert.assertNotNull(result.get(0));
        Assert.assertNotNull(result.get(0).getId());
        Assert.assertEquals(CAPTURE_TIME3, result.get(0).getId().getCaptureTime());
        Assert.assertEquals("apdwa571", result.get(0).getId().getServer());
        Assert.assertNull(result.get(0).getId().getPortMapId());
    }

    @Test
    public void testSelectAll()
    {
        persist();
        String query2 = "select x from NetstatData x " + "where x.id.portMapTypeCd= :port";

        Query query = em.createQuery(query2);
        query.setParameter("port", "portMapTypeCd");
        List<NetstatData> result = query.getResultList();
        Assert.assertNotNull(result);
        Assert.assertEquals(4, result.size());
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
        if (cli != null)
        {
            cli.dropTable("KunderaExamples");
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

        NetstatData data1 = new NetstatData();
        NetstatDataId id1 = new NetstatDataId();
        id1.setServer("apdwa570");
        data1.setRowKey("rowKey2");
        id1.setCaptureTime(CAPTURE_TIME1);
        id1.setPortMapTypeCd("portMapTypeCd");
        data1.setEstablished(2);
        data1.setTotal(2);
        data1.setId(id1);

        em.persist(data1);

        NetstatData data2 = new NetstatData();
        NetstatDataId id2 = new NetstatDataId();
        id2.setServer("apdwa571");
        data2.setRowKey("rowKey3");
        id2.setCaptureTime(CAPTURE_TIME2);
        id2.setPortMapTypeCd("portMapTypeCd");
        data2.setEstablished(3);
        data2.setTotal(3);
        data2.setId(id2);

        em.persist(data2);

        NetstatData data3 = new NetstatData();
        NetstatDataId id3 = new NetstatDataId();
        id3.setServer("apdwa571");
        data3.setRowKey("rowKey4");
        id3.setCaptureTime(CAPTURE_TIME3);
        id3.setPortMapTypeCd("portMapTypeCd");
        data3.setEstablished(4);
        data3.setTotal(4);
        data3.setId(id3);

        em.persist(data3);

        em.clear();
    }
}
