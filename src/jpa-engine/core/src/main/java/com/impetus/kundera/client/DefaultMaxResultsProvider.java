package com.impetus.kundera.client;

/**
 * Defines a method for clients to override the default maxResults setting for queries.
 */
public interface DefaultMaxResultsProvider {

   /**
    * Returns the default maxResults setting to apply for queries.
    *
    * @return the default maxResults setting to apply for queries
    */
   int getDefaultMaxResults();

}
