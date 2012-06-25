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
package com.impetus.kundera.rest.resources;

import javax.ws.rs.core.MediaType;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.rest.common.Constants;
import com.impetus.kundera.rest.common.JAXBUtils;
import com.impetus.kundera.rest.common.StreamUtils;
import com.impetus.kundera.rest.dao.RESTClient;
import com.impetus.kundera.rest.dao.RESTClientImpl;
import com.impetus.kundera.rest.dto.SchemaMetadata;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.test.framework.JerseyTest;

/**
 * Test case for {@link MetadataResource}
 * 
 * @author amresh
 * 
 */
public class MetadataResourceTest extends JerseyTest
{

    String mediaType = MediaType.APPLICATION_XML;

    RESTClient restClient;

    public MetadataResourceTest() throws Exception
    {
        super(Constants.KUNDERA_REST_RESOURCES_PACKAGE);
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        super.setUp();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        super.tearDown();
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.rest.resources.MetadataResource#getSchemaList(java.lang.String)}
     * .
     */
    @Test
    public void testGetSchemaList()
    {
        WebResource webResource = resource();

        RESTClient restClient = new RESTClientImpl();
        // Initialize REST Client
        restClient.initialize(webResource, mediaType);

        // Get Application Token
        String applicationToken = restClient.getApplicationToken("twissandra");
        Assert.assertNotNull(applicationToken);
        Assert.assertTrue(applicationToken.startsWith("AT_"));

        String schemaList = restClient.getSchemaList("twissandra");
        Assert.assertNotNull(schemaList);

        SchemaMetadata sm = (SchemaMetadata) JAXBUtils.toObject(StreamUtils.toInputStream(schemaList),
                SchemaMetadata.class, mediaType);
        Assert.assertNotNull(sm);
        Assert.assertNotNull(sm.getSchemaList());
        Assert.assertFalse(sm.getSchemaList().isEmpty());
        Assert.assertEquals("KunderaExamples", sm.getSchemaList().get(0).getSchemaName());

    }

}
