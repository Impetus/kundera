package com.impetus.client.es.mappedsuperclass;

import java.util.List;

import javax.persistence.Query;

import junit.framework.Assert;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.client.crud.mappedsuperclass.CreditTransaction;
import com.impetus.kundera.client.crud.mappedsuperclass.MappedSuperClassBase;
import com.impetus.kundera.metadata.model.KunderaMetadata;

public class ESMappedSuperClassTest extends MappedSuperClassBase
{

    private static Node node = null;

    @Before
    public void setUp() throws Exception
    {
        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder();
        builder.put("path.data", "target/data");
        node = new NodeBuilder().settings(builder).node();
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
        node.close();
        tearDownInternal();
        KunderaMetadata.INSTANCE.setApplicationMetadata(null);

    }
}
