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

package com.impetus.client.mongodb.query.gfs;

import com.mongodb.AggregationOutput;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;

import java.util.ArrayList;
import java.util.List;

/**
 * Extension over the native GridFS implementation to provide more efficient
 * queries with offsets and limits.
 */
public class KunderaGridFS extends GridFS
{

    /**
     * Creates a GridFS instance for the default bucket "fs" in the given
     * database. Set the preferred WriteConcern on the give DB with
     * DB.setWriteConcern
     * 
     * @param db
     *            database to work with
     * @see com.mongodb.WriteConcern
     */
    public KunderaGridFS(final DB db)
    {
        super(db);
    }

    /**
     * Creates a GridFS instance for the specified bucket in the given database.
     * Set the preferred WriteConcern on the give DB with DB.setWriteConcern
     * 
     * @param db
     *            database to work with
     * @param bucket
     *            bucket to use in the given database
     * @see com.mongodb.WriteConcern
     */
    public KunderaGridFS(final DB db, final String bucket)
    {
        super(db, bucket);
    }

    /**
     * Finds a list of files matching the given query.
     * 
     * @param query
     *            the filter to apply
     * @param sort
     *            the fields to sort with
     * @param firstResult
     *            number of files to skip
     * @param maxResult
     *            number of files to return
     * @return list of gridfs files
     */
    public List<GridFSDBFile> find(final DBObject query, final DBObject sort, final int firstResult,
            final int maxResult)
    {
        List<GridFSDBFile> files = new ArrayList<GridFSDBFile>();

        DBCursor c = null;
        try
        {
            c = getFilesCollection().find(query);
            if (sort != null)
            {
                c.sort(sort);
            }
            c.skip(firstResult).limit(maxResult);
            while (c.hasNext())
            {
                files.add(findOne(c.next()));
            }
        }
        finally
        {
            if (c != null)
            {
                c.close();
            }
        }
        return files;
    }

    /**
     * MongoDB aggregation.
     *
     * @param pipeline
     * @return
     */
    public AggregationOutput aggregate(List<DBObject> pipeline)
    {
        return getFilesCollection().aggregate(pipeline);
    }

}