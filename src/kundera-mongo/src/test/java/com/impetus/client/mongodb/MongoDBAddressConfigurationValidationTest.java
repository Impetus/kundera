package com.impetus.client.mongodb;

import org.junit.Test;

/**
 * Tests the MongoDB client factory configuration validation for node hostnames and ports.
 */
public class MongoDBAddressConfigurationValidationTest
{

   private final MongoDBClientFactory factory = new MongoDBClientFactory();

   @Test
   public void acceptsSingleHostWithDefaultPort()
   {
      factory.onValidation("localhost", "27017");
   }

   @Test
   public void acceptsHostListWithDefaultPort()
   {
      factory.onValidation("node001,node002", "27017");
   }

   @Test
   public void acceptsHostListWithoutDefaultPort()
   {
      factory.onValidation("node001:27001,node002:27002", null);
   }

   @Test(expected = IllegalArgumentException.class)
   public void failsIfAnyHostIsMissingPortWithoutDefaultPort()
   {
      factory.onValidation("node001:27001,node002", null);
   }

   @Test
   public void acceptsIPv6AddressesWithDefaultPort()
   {
      factory.onValidation("2001:db8:85a3::8a2e:370:7334,2001:db8::12", "27099");
   }

   @Test(expected = IllegalArgumentException.class)
   public void failsWithIPv6AddressesWithoutDefaultPort()
   {
      factory.onValidation("2001:db8:85a3::8a2e:370:7334,2001:db8::12", null);
   }

}