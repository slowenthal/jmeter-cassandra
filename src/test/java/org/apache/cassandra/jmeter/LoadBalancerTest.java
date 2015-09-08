package org.apache.cassandra.jmeter;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.*;
import org.apache.cassandra.jmeter.config.CassandraConnection;
import org.testng.ITestResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * DataStax Academy Sample Application
 * <p/>
 * Copyright 2013 DataStax
 */
public class LoadBalancerTest extends JMeterTest {

    // TODO - sort out BeforeClass hierarchies
    private CassandraConnection cc;

    @BeforeClass
    public void beforeClass() {
        super.beforeClass();
    }

    @AfterMethod
    public void afterMethod(ITestResult result) {
        if (cc != null)
            cc.testEnded();
    }

    @Test
    public void testRoundRobin() {
        final String SESSION_NAME = "lb4";

        cc = new CassandraConnection();

        cc.setProperty("contactPoints", NODE_1_IP);
        cc.setProperty("loadBalancer", CassandraConnection.ROUND_ROBIN);
        cc.setProperty("sessionName", SESSION_NAME);

        cc.testStarted();

        Session session = CassandraConnection.getSession(SESSION_NAME);
        assertNotNull(session);

        LoadBalancingPolicy loadBalancer = session.getCluster().getConfiguration().getPolicies().getLoadBalancingPolicy();
        assertEquals(loadBalancer.getClass(), RoundRobinPolicy.class);

        // check that we can select from system.local
        String clusterName = session.execute("select cluster_name from system.local where key ='local'").one().getString(0);
        assertEquals(clusterName,"test");

        cc.testEnded();

        // Are we closed?
        assertTrue(session.isClosed(), "Session is Closed");

    }

    @Test
    public void testDefaultTwoContactPoints() {
        final String SESSION_NAME = "lb7";

        cc = new CassandraConnection();

        cc.setProperty("contactPoints", "127.0.1.2," + NODE_1_IP);
        cc.setProperty("sessionName", SESSION_NAME);

        cc.testStarted();

        Session session = CassandraConnection.getSession(SESSION_NAME);
        assertNotNull(session);

        LoadBalancingPolicy loadBalancer = session.getCluster().getConfiguration().getPolicies().getLoadBalancingPolicy();
        assertEquals(loadBalancer.getClass(), TokenAwarePolicy.class);

        // check that we can select from system.local
        String clusterName = session.execute("select cluster_name from system.local where key ='local'").one().getString(0);
        assertEquals(clusterName,"test");

        cc.testEnded();

        // Are we closed?
        assertTrue(session.isClosed(), "Session is Closed");

    }

    @Test
    public void testDCAwareRoundRobinDefault() {

        cc = new CassandraConnection();

        cc.setProperty("contactPoints", NODE_1_IP);
        cc.setProperty("loadBalancer", CassandraConnection.DC_AWARE_ROUND_ROBIN);
        cc.setProperty("sessionName", "lb0");

        cc.testStarted();

        Session session = CassandraConnection.getSession("lb0");
        assertNotNull(session);

        LoadBalancingPolicy loadBalancer = session.getCluster().getConfiguration().getPolicies().getLoadBalancingPolicy();
        assertEquals(loadBalancer.getClass(), DCAwareRoundRobinPolicy.class);

        // check that we can select from system.local
        String clusterName = session.execute("select cluster_name from system.local where key ='local'").one().getString(0);
        assertEquals(clusterName,"test");

        cc.testEnded();

        // Are we closed?
        assertTrue(session.isClosed(), "Session is Closed");

    }

    @Test
    public void testDCAwareRoundRobinDatacenter1() {

        CassandraConnection cc = new CassandraConnection();

        cc.setProperty("contactPoints", NODE_1_IP);
        cc.setProperty("loadBalancer", CassandraConnection.DC_AWARE_ROUND_ROBIN);
        cc.setProperty("localDataCenter", "datacenter1");
        cc.setProperty("sessionName", "lb1");

        cc.testStarted();

        Session session = CassandraConnection.getSession("lb1");
        assertNotNull(session);

        LoadBalancingPolicy loadBalancer = session.getCluster().getConfiguration().getPolicies().getLoadBalancingPolicy();
        assertEquals(loadBalancer.getClass(), DCAwareRoundRobinPolicy.class);

        // check that we can select from system.local
        String clusterName = session.execute("select cluster_name from system.local where key ='local'").one().getString(0);
        assertEquals(clusterName,"test");

        cc.testEnded();

        // Are we closed?
        assertTrue(session.isClosed(), "Session is Closed");

    }

    @Test (expectedExceptions = NoHostAvailableException.class)
    public void testDCAwareRoundRobinDatacenter2() {

        CassandraConnection cc = new CassandraConnection();

        cc.setProperty("contactPoints", NODE_1_IP);
        cc.setProperty("loadBalancer", CassandraConnection.DC_AWARE_ROUND_ROBIN);
        cc.setProperty("localDataCenter", "datacenter2");
        cc.setProperty("sessionName", "lb2");

        cc.testStarted();

        Session session = CassandraConnection.getSession("lb2");
        assertNotNull(session);

        LoadBalancingPolicy loadBalancer = session.getCluster().getConfiguration().getPolicies().getLoadBalancingPolicy();
        assertEquals(loadBalancer.getClass(), DCAwareRoundRobinPolicy.class);

        // check that we can select from system.local
        String clusterName = session.execute("select cluster_name from system.local where key ='local'").one().getString(0);
        assertEquals(clusterName,"test");

        cc.testEnded();

        // Are we closed?
        assertTrue(session.isClosed(), "Session is Closed");

    }


    @Test
    public void testWhiteList() {
        final String SESSION_NAME = "lb6";

        cc = new CassandraConnection();

        cc.setProperty("contactPoints", NODE_1_IP);
        cc.setProperty("loadBalancer", CassandraConnection.WHITELIST);
        cc.setProperty("sessionName", SESSION_NAME);

        cc.testStarted();

        Session session = CassandraConnection.getSession(SESSION_NAME);
        assertNotNull(session);

        LoadBalancingPolicy loadBalancer = session.getCluster().getConfiguration().getPolicies().getLoadBalancingPolicy();
        assertEquals(loadBalancer.getClass(), WhiteListPolicy.class);

        // check that we can select from system.local
        String clusterName = session.execute("select cluster_name from system.local where key ='local'").one().getString(0);
        assertEquals(clusterName,"test");

        cc.testEnded();

        // Are we closed?
        assertTrue(session.isClosed(), "Session is Closed");

    }

    @Test
    public void testTokenAware() {
        final String SESSION_NAME = "lb5";

        cc = new CassandraConnection();

        cc.setProperty("contactPoints", NODE_1_IP);
        cc.setProperty("loadBalancer", CassandraConnection.DC_TOKEN_AWARE);
        cc.setProperty("sessionName", SESSION_NAME);

        cc.testStarted();

        Session session = CassandraConnection.getSession(SESSION_NAME);
        assertNotNull(session);

        LoadBalancingPolicy loadBalancer = session.getCluster().getConfiguration().getPolicies().getLoadBalancingPolicy();
        assertEquals(loadBalancer.getClass(), TokenAwarePolicy.class);

        // check that we can select from system.local
        String clusterName = session.execute("select cluster_name from system.local where key ='local'").one().getString(0);
        assertEquals(clusterName,"test");

        cc.testEnded();

        // Are we closed?
        assertTrue(session.isClosed(), "Session is Closed");

    }

}
