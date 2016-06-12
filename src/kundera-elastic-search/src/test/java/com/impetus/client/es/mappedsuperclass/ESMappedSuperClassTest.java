package com.impetus.client.es.mappedsuperclass;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.client.crud.mappedsuperclass.MappedSuperClassBase;

public class ESMappedSuperClassTest extends MappedSuperClassBase
{

    private static Node node;

    @Before
    public void setUp() throws Exception
    {
        if (!checkIfServerRunning())
        {
            Builder builder = Settings.settingsBuilder();
            builder.put("path.home", "target/data");
            node = new NodeBuilder().settings(builder).node();
        }
        _PU = "es-pu";
        setUpInternal();
    }

    @Test
    public void test()
    {
        assertInternal(true);
    }

    @After
    public void tearDown() throws Exception
    {
        if (checkIfServerRunning() && node != null)
        {
            node.close();
        }
        tearDownInternal();

    }

    /**
     * Check if server running.
     * 
     * @return true, if successful
     */
    private static boolean checkIfServerRunning()
    {
        try
        {
            Socket socket = new Socket("127.0.0.1", 9300);
            return socket.getInetAddress() != null;
        }
        catch (UnknownHostException e)
        {
            return false;
        }
        catch (IOException e)
        {
            return false;
        }

    }
}