package org.apache.cassandra.jmeter;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import org.apache.cassandra.jmeter.config.CassandraConnection;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * DataStax Academy Sample Application
 * <p/>
 * Copyright 2013 DataStax
 */
public class ConnectionTest extends JMeterTest {

    // TODO - sort out BeforeClass hierarchies

    @BeforeClass
    public void beforeClass() {
        super.beforeClass();
    }

    @Test
    public void testConnection() {
        CassandraConnection cc = new CassandraConnection();

        cc.setProperty("contactPoints", NODE_1_IP);
//        cc.setProperty("keyspace", "testks");
        cc.setProperty("sessionName", "testsession");

        cc.testStarted();

        Session session = CassandraConnection.getSession("testsession");
        assertNotNull(session);

        // check that we can select from system.local
        String clusterName = session.execute("select cluster_name from system.local where key ='local'").one().getString(0);

        assertEquals(clusterName,"test");

        cc.testEnded();

        // Are we closed?
        assertTrue(session.isClosed(), "Session is Closed");

    }

    @Test
    public void testBadConnection() {
        CassandraConnection cc = new CassandraConnection();

        cc.setProperty("contactPoints", "127.1.1.1");
//        cc.setProperty("keyspace", "testks");
        cc.setProperty("sessionName", "testsession");

        Boolean exeptionCaught=false;

        try {
            cc.testStarted();
        } catch (NoHostAvailableException e) {
            exeptionCaught = true;
        }
        assertTrue(exeptionCaught, "NoHostAvailable did not occur.");

        cc.testEnded();
    }
}
