package org.apache.cassandra.jmeter;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import org.apache.cassandra.jmeter.config.CassandraConnection;
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

    @BeforeClass
    public void beforeClass() {
        super.beforeClass();
    }

    @Test
    public void testDCAwareRoundRobin() {

        // TODO - fill this in
//        CassandraConnection cc = new CassandraConnection();
//
//        cc.setProperty("contactPoints", NODE_1_IP);
//
//        cc.setProperty("sessionName", "testsession");
//
//        cc.testStarted();
//
//        Session session = CassandraConnection.getSession("testsession");
//        assertNotNull(session);
//
//        // check that we can select from system.local
//        String clusterName = session.execute("select cluster_name from system.local where key ='local'").one().getString(0);
//
//        assertEquals(clusterName,"test");
//
//        cc.testEnded();
//
//        // Are we closed?
//        assertTrue(session.isClosed(), "Session is Closed");
//
    }

}
