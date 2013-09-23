/**
 * Copyright 2013 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera.gis.geometry;

import junit.framework.Assert;

import org.junit.Test;

/**
 * @author vivek.mishra
 * junit for {@link Envelope}.
 */
public class EnvelopeTest
{

    @Test
    public void test()
    {
        Coordinate coordiates2d = new Coordinate(34.2d,34.4d);

        Coordinate coordiates3d = new Coordinate(34.2d,34.4d,12.3d);

        Envelope envelope = new Envelope(coordiates2d,coordiates3d);
        Assert.assertTrue(envelope.contains(coordiates3d));
        Assert.assertTrue(envelope.covers(coordiates3d));

        envelope = new Envelope(34.2d,34.4d,12.3d,32d);
        Assert.assertFalse(envelope.contains(coordiates3d));
        Assert.assertFalse(envelope.covers(coordiates3d));
        Assert.assertNotNull(envelope.centre());
        
        envelope = new Envelope();
        Assert.assertEquals(0d,envelope.getArea());
        envelope = new Envelope(coordiates3d);
        Assert.assertNotNull(envelope.getArea());
        com.vividsolutions.jts.geom.Envelope rawEnvelope = new com.vividsolutions.jts.geom.Envelope();
        envelope = new Envelope(rawEnvelope);
        Assert.assertNull(envelope.centre());

        
    }

}
