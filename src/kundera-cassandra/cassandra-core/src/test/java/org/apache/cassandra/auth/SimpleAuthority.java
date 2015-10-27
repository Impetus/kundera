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
package org.apache.cassandra.auth;

/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */

import java.io.IOException;
import java.io.InputStream;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.io.util.FileUtils;

// TODO: Auto-generated Javadoc
/**
 * The Class SimpleAuthority.
 */
public class SimpleAuthority implements IAuthorizer {

    /** The Constant ACCESS_FILENAME_PROPERTY. */
    public final static String ACCESS_FILENAME_PROPERTY = "access.properties";

    // magical property for WRITE permissions to the keyspaces list
    /** The Constant KEYSPACES_WRITE_PROPERTY. */
    public final static String KEYSPACES_WRITE_PROPERTY = "<modify-keyspaces>";

    /**
     * Authorize.
     * 
     * @param user
     *            the user
     * @param resource
     *            the resource
     * @return the enum set
     */
    public EnumSet<Permission> authorize(AuthenticatedUser user, List<Object> resource) {
        if (resource.size() < 2 || !Resources.ROOT.equals(resource.get(0))
            || !Resources.KEYSPACES.equals(resource.get(1)))
            return (EnumSet<Permission>) Permission.NONE;

        String keyspace, columnFamily = null;
        EnumSet<Permission> authorized = (EnumSet<Permission>) Permission.NONE;

        // /cassandra/keyspaces
        if (resource.size() == 2) {
            keyspace = KEYSPACES_WRITE_PROPERTY;
            authorized = EnumSet.of(Permission.READ);
        }
        // /cassandra/keyspaces/<keyspace name>
        else if (resource.size() == 3) {
            keyspace = (String) resource.get(2);
        }
        // /cassandra/keyspaces/<keyspace name>/<cf name>
        else if (resource.size() == 4) {
            keyspace = (String) resource.get(2);
            columnFamily = (String) resource.get(3);
        } else {
            // We don't currently descend any lower in the hierarchy.
            throw new UnsupportedOperationException();
        }

        String accessFilename = System.getProperty(ACCESS_FILENAME_PROPERTY);
        InputStream in = null;
        try {
            in = Thread.currentThread().getContextClassLoader().getResourceAsStream("access.properties");
            // in = new BufferedInputStream(new
            // FileInputStream(accessFilename));
            Properties accessProperties = new Properties();
            accessProperties.load(in);

            // Special case access to the keyspace list
            if (keyspace == KEYSPACES_WRITE_PROPERTY) {
                String kspAdmins = accessProperties.getProperty(KEYSPACES_WRITE_PROPERTY);
                for (String admin : kspAdmins.split(","))
                    if (admin.equals(user.getName()))
                        return (EnumSet<Permission>) Permission.ALL;
            }

            boolean canRead = false, canWrite = false;
            String readers = null, writers = null;

            if (columnFamily == null) {
                readers = accessProperties.getProperty(keyspace + ".<ro>");
                writers = accessProperties.getProperty(keyspace + ".<rw>");
            } else {
                readers = accessProperties.getProperty(keyspace + "." + columnFamily + ".<ro>");
                writers = accessProperties.getProperty(keyspace + "." + columnFamily + ".<rw>");
            }

            if (readers != null) {
                for (String reader : readers.split(",")) {
                    if (reader.equals(user.getName())) {
                        canRead = true;
                        break;
                    }
                }
            }

            if (writers != null) {
                for (String writer : writers.split(",")) {
                    if (writer.equals(user.getName())) {
                        canWrite = true;
                        break;
                    }
                }
            }

            if (canWrite)
                authorized = (EnumSet<Permission>) Permission.ALL;
            else if (canRead)
                authorized = EnumSet.of(Permission.READ);

        } catch (IOException e) {
            throw new RuntimeException(String.format("Authorization table file '%s' could not be opened: %s",
                accessFilename, e.getMessage()));
        } finally {
            FileUtils.closeQuietly(in);
        }

        return authorized;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.cassandra.auth.IAuthorizer#validateConfiguration()
     */
    public void validateConfiguration() throws ConfigurationException {
        String afilename = System.getProperty(ACCESS_FILENAME_PROPERTY);
        if (afilename == null) {
            throw new ConfigurationException(String.format("When using %s, '%s' property must be defined.", this
                .getClass().getCanonicalName(), ACCESS_FILENAME_PROPERTY));
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.cassandra.auth.IAuthorizer#authorize(org.apache.cassandra. auth.AuthenticatedUser,
     * org.apache.cassandra.auth.IResource)
     */
    @Override
    public Set<Permission> authorize(AuthenticatedUser arg0, IResource arg1) {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.cassandra.auth.IAuthorizer#protectedResources()
     */
    @Override
    public Set<? extends IResource> protectedResources() {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.cassandra.auth.IAuthorizer#setup()
     */
    @Override
    public void setup() {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.cassandra.auth.IAuthorizer#grant(org.apache.cassandra.auth.AuthenticatedUser, java.util.Set,
     * org.apache.cassandra.auth.IResource, org.apache.cassandra.auth.RoleResource)
     */
    @Override
    public void grant(AuthenticatedUser arg0, Set<Permission> arg1, IResource arg2, RoleResource arg3)
        throws RequestValidationException, RequestExecutionException {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.cassandra.auth.IAuthorizer#list(org.apache.cassandra.auth.AuthenticatedUser, java.util.Set,
     * org.apache.cassandra.auth.IResource, org.apache.cassandra.auth.RoleResource)
     */
    @Override
    public Set<PermissionDetails> list(AuthenticatedUser arg0, Set<Permission> arg1, IResource arg2, RoleResource arg3)
        throws RequestValidationException, RequestExecutionException {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.cassandra.auth.IAuthorizer#revoke(org.apache.cassandra.auth.AuthenticatedUser, java.util.Set,
     * org.apache.cassandra.auth.IResource, org.apache.cassandra.auth.RoleResource)
     */
    @Override
    public void revoke(AuthenticatedUser arg0, Set<Permission> arg1, IResource arg2, RoleResource arg3)
        throws RequestValidationException, RequestExecutionException {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.cassandra.auth.IAuthorizer#revokeAllFrom(org.apache.cassandra.auth.RoleResource)
     */
    @Override
    public void revokeAllFrom(RoleResource arg0) {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.cassandra.auth.IAuthorizer#revokeAllOn(org.apache.cassandra.auth.IResource)
     */
    @Override
    public void revokeAllOn(IResource arg0) {
        // TODO Auto-generated method stub

    }
}