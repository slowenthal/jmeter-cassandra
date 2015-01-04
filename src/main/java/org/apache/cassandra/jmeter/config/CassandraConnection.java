package org.apache.cassandra.jmeter.config;
/*
 * Copyright 2014 Steven Lowenthal
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

import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;
import org.apache.jmeter.config.ConfigElement;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jmeter.testbeans.TestBeanHelper;
import org.apache.jmeter.testelement.AbstractTestElement;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.threads.JMeterVariables;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;

public class CassandraConnection extends AbstractTestElement
    implements ConfigElement, TestStateListener, TestBean
    {

    // Load Balancer constants
    static final String ROUND_ROBIN = "RoundRobin";
    static final String DC_AWARE_ROUND_ROBIN = "DCAwareRoundRobin";
    static final String WHITELIST = "WhiteListRoundRobin";
    static final String DEFAULTLOADBALANCER = "Default";

    private static final Logger log = LoggingManager.getLoggerForClass();

    private static final long serialVersionUID = 233L;

    private transient String contactPoints, keyspace, username, password, sessionName, loadBalancer, localDataCenter;

    private final transient Set<InetAddress> contactPointsI = new HashSet<InetAddress>();
    private final transient Set<InetSocketAddress> contactPointsIS = new HashSet<InetSocketAddress>();

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
          CassandraSessionFactory.destroyClusters();
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
        LoadBalancingPolicy loadBalancingPolicy = null;

        if (loadBalancer.contentEquals(DC_AWARE_ROUND_ROBIN)) {
            // in driver v2.0.2+, we can use the default constructor on
            // dcawareroundrobinpolicy
            if (localDataCenter.isEmpty()) {
                loadBalancingPolicy = new DCAwareRoundRobinPolicy();
            }   else {
                loadBalancingPolicy = new DCAwareRoundRobinPolicy(localDataCenter);
            }
        } else if (loadBalancer.contentEquals(WHITELIST)) {
            loadBalancingPolicy = new WhiteListPolicy(new RoundRobinPolicy(), contactPointsIS);
        } else if (loadBalancer.contentEquals(ROUND_ROBIN)) {
            loadBalancingPolicy = new RoundRobinPolicy();
        } else if (loadBalancer.contentEquals(DEFAULTLOADBALANCER)) {
            loadBalancingPolicy = null;
        }

        Session session = CassandraSessionFactory.createSession(sessionName, contactPointsI, keyspace, username, password, loadBalancingPolicy);

        variables.putObject(sessionName, session);
    }

    @Override
    public void testStarted(String host) {
        testStarted();
    }

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
        return contactPoints.toString();
    }

    /**
     * @param contactPoints
     *            The poolname to set.
     */
    public void setContactPoints(String contactPoints) throws UnknownHostException {
        this.contactPoints = contactPoints;
        for (String contactPt : contactPoints.split(",")) {
            this.contactPointsI.add(InetAddress.getByName(contactPt));
            // TODO - 9160 should not really be hard coded.
            this.contactPointsIS.add(InetSocketAddress.createUnresolved(contactPt, 9160));
        }
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

   public String getLoadBalancer() {
       return loadBalancer;
   }

   public void setLoadBalancer(String loadBalancer) {
       this.loadBalancer = loadBalancer;
   }

    public String getLocalDataCenter() {
        return localDataCenter;
    }

    public void setLocalDataCenter(String localDataCenter) {
        this.localDataCenter = localDataCenter;
    }
 }
