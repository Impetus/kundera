package com.impetus.client.mongodb.query.gfs;

import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;

import java.util.ArrayList;
import java.util.List;

/**
 * Extension over the native GridFS implementation to provide more efficient queries with offsets and limits.
 */
public class KunderaGridFS extends GridFS {

   /**
    * Creates a GridFS instance for the default bucket "fs" in the given database. Set the preferred WriteConcern on the give DB with
    * DB.setWriteConcern
    *
    * @param db database to work with
    * @throws com.mongodb.MongoException
    * @see com.mongodb.WriteConcern
    */
   public KunderaGridFS(final DB db) {
      super(db);
   }

   /**
    * Creates a GridFS instance for the specified bucket in the given database.  Set the preferred WriteConcern on the give DB with
    * DB.setWriteConcern
    *
    * @param db database to work with
    * @param bucket bucket to use in the given database
    * @throws com.mongodb.MongoException
    * @see com.mongodb.WriteConcern
    */
   public KunderaGridFS(final DB db, final String bucket) {
      super(db, bucket);
   }

   /**
    * Finds a list of files matching the given query.
    *
    * @param query the filter to apply
    * @param sort  the fields to sort with
    * @param firstResult number of files to skip
    * @param maxResult   number of files to return
    * @return list of gridfs files
    * @throws com.mongodb.MongoException
    */
   public List<GridFSDBFile> find(final DBObject query, final DBObject sort, final int firstResult, final int maxResult) {
      List<GridFSDBFile> files = new ArrayList<GridFSDBFile>();

      DBCursor c = null;
      try {
         c = _filesCollection.find( query );
         if (sort != null) {
            c.sort(sort);
         }
         c.skip( firstResult ).limit( maxResult );
         while ( c.hasNext() ){
            files.add( _fix( c.next() ) );
         }
      } finally {
         if (c != null){
            c.close();
         }
      }
      return files;
   }

}
