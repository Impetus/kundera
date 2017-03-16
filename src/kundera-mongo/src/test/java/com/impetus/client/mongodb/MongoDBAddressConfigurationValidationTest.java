/*******************************************************************************
 * * Copyright 2017 Impetus Infotech.
 * *
 * * Licensed under the Apache License, Version 2.0 (the "License");
 * * you may not use this file except in compliance with the License.
 * * You may obtain a copy of the License at
 * *
 * * http://www.apache.org/licenses/LICENSE-2.0
 * *
 * * Unless required by applicable law or agreed to in writing, software
 * * distributed under the License is distributed on an "AS IS" BASIS,
 * * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * * See the License for the specific language governing permissions and
 * * limitations under the License.
 ******************************************************************************/
package com.impetus.client.mongodb;

import org.junit.Test;

/**
 * Tests the MongoDB client factory configuration validation for node hostnames
 * and ports.
 */
public class MongoDBAddressConfigurationValidationTest
{

    /** The factory. */
    private final MongoDBClientFactory factory = new MongoDBClientFactory();

    /**
     * Accepts single host with default port.
     */
    @Test
    public void acceptsSingleHostWithDefaultPort()
    {
        factory.onValidation("localhost", "27017");
    }

    /**
     * Accepts host list with default port.
     */
    @Test
    public void acceptsHostListWithDefaultPort()
    {
        factory.onValidation("node001,node002", "27017");
    }

    /**
     * Accepts host list without default port.
     */
    @Test
    public void acceptsHostListWithoutDefaultPort()
    {
        factory.onValidation("node001:27001,node002:27002", null);
    }

    /**
     * Fails if any host is missing port without default port.
     */
    @Test(expected = IllegalArgumentException.class)
    public void failsIfAnyHostIsMissingPortWithoutDefaultPort()
    {
        factory.onValidation("node001:27001,node002", null);
    }

    /**
     * Accepts i pv6 addresses with default port.
     */
    @Test
    public void acceptsIPv6AddressesWithDefaultPort()
    {
        factory.onValidation("2001:db8:85a3::8a2e:370:7334,2001:db8::12", "27099");
    }

    /**
     * Fails with i pv6 addresses without default port.
     */
    @Test(expected = IllegalArgumentException.class)
    public void failsWithIPv6AddressesWithoutDefaultPort()
    {
        factory.onValidation("2001:db8:85a3::8a2e:370:7334,2001:db8::12", null);
    }

}