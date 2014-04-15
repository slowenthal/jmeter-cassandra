package org.apache.cassandra.jmeter.config;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.datastax.driver.core.Session;
import org.apache.jmeter.config.ConfigElement;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jmeter.testbeans.TestBeanHelper;
import org.apache.jmeter.testelement.AbstractTestElement;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.threads.JMeterVariables;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import java.util.*;

public class CassandraConnection extends AbstractTestElement
    implements ConfigElement, TestStateListener, TestBean
    {
    private static final Logger log = LoggingManager.getLoggerForClass();

    private static final long serialVersionUID = 233L;

    private transient String contactPoints, keyspace, username, password, sessionName;

    // TODO - Add Port Number

    /*
     *  The datasource is set up by testStarted and cleared by testEnded.
     *  These are called from different threads, so access must be synchronized.
     *  The same instance is called in each case.
    */

    // Keep a record of the pre-thread pools so that they can be disposed of at the end of a test

    public CassandraConnection() {
    }

    @Override
    public void testEnded() {

        // TODO - shut down connections
    }

    @Override
    public void testEnded(String host) {
        testEnded();
    }

    @Override
    @SuppressWarnings("deprecation") // call to TestBeanHelper.prepare() is intentional
    public void testStarted() {
        this.setRunningVersion(true);
        TestBeanHelper.prepare(this);
        JMeterVariables variables = getThreadContext().getVariables();

        Session session = CassandraSessionFactory.createSession(contactPoints, keyspace, null);

        variables.putObject(sessionName, session);
    }

    @Override
    public void testStarted(String host) {
        testStarted();
    }

      // TODO - Fix this
    @Override
    public Object clone() {
        return (CassandraConnection) super.clone();
    }

    /*
     * Utility routine to get the connection from the pool.
     * Purpose:
     * - allows CassandraSampler to be entirely independent of the pooling classes
     * - allows the pool storage mechanism to be changed if necessary
     */
    public static Session getSession(String sessionName) {
                return (Session) JMeterContextService.getContext().getVariables().getObject(sessionName);
     }

    // used to hold per-thread singleton connection pools
    private static final ThreadLocal<Map<String, Session>> perThreadPoolMap =
        new ThreadLocal<Map<String, Session>>(){
        @Override
        protected Map<String, Session> initialValue() {
            return new HashMap<String, Session>();
        }
    };



    @Override
    public void addConfigElement(ConfigElement config) {
    }

    @Override
    public boolean expectsModification() {
        return false;
    }


    /**
     * @return Returns the poolname.
     */
    public String getContactPoints() {
        return contactPoints;
    }

    /**
     * @param contactPoints
     *            The poolname to set.
     */
    public void setContactPoints(String contactPoints) {
        this.contactPoints = contactPoints;
    }

    /**
     * @return Returns the keyspace.
     */
    public String getKeyspace() {
        return keyspace;
    }

    /**
     * @param keyspace
     *            The keyspace to set.
     */
    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    /**
     * @return Returns the password.
     */
    public String getPassword() {
        return password;
    }

    /**
     * @param password
     *            The password to set.
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * @return Returns the username.
     */
    public String getUsername() {
        return username;
    }

    /**
     * @param username
     *            The username to set.
     */
    public void setUsername(String username) {
        this.username = username;
    }


   public String getSessionName() {
       return sessionName;
   }

   public void setSessionName(String sessionName) {
       this.sessionName = sessionName;
   }
}
