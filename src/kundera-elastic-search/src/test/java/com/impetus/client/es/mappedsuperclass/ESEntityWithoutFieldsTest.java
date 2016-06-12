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

import com.impetus.kundera.metadata.mappedsuperclass.EntityWithoutFieldsBase;

/**
 * @author amitkumar
 * 
 *         Class to verify that at least one field in entity class is not
 *         mandatory if the superclass contains all the mandatory fields
 */
public class ESEntityWithoutFieldsTest extends EntityWithoutFieldsBase
{

    private static Node node;

    @Before
    public void setup()
    {

        if (!checkIfServerRunning())
        {
            Builder builder = Settings.settingsBuilder();
            builder.put("path.home", "target/data");
            node = new NodeBuilder().settings(builder).node();
        }
        persistenceUnit = "esMappedSuperClass-pu";
        setupInternal();
    }

    @Test
    public void testEntityWithNoFields()
    {
        testEntityWithNoFieldsBase();
    }

    @Test
    public void testEntityWithNoFields2LevelInheritance()
    {
        testEntityWithNoFields2LevelInheritanceBase();
    }

    @After
    public void tearDown()
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