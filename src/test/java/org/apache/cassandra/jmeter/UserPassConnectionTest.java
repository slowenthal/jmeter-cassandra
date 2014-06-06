package org.apache.cassandra.jmeter;

import com.datastax.driver.core.CCMBridge;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import org.apache.cassandra.jmeter.config.CassandraConnection;
import org.apache.cassandra.jmeter.config.CassandraSessionFactory;
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
public class UserPassConnectionTest extends JMeterTest {

    @BeforeClass
    public void beforeClass() {
        super.beforeClass();
        cassandraCluster.updateConfig("authenticator", "PasswordAuthenticator");
        cassandraCluster.updateConfig("authorizer", "CassandraAuthorizer");
        cassandraCluster.stop();
        cassandraCluster.start();
        try {
            Thread.sleep(60000);
        } catch (InterruptedException e) {

        }
    }

    @Test
    public void testCorrectUsernamePassword() {
        CassandraConnection cc = new CassandraConnection();

        cc.setProperty("contactPoints", NODE_1_IP);
        cc.setProperty("sessionName", "testsession");
        cc.setProperty("username", "cassandra");
        cc.setProperty("password", "cassandra");

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
    public void testNoUsername() {
        CassandraConnection cc = new CassandraConnection();

        cc.setProperty("contactPoints", NODE_1_IP);
        cc.setProperty("sessionName", "testsession2");

        Boolean exeptionCaught=false;

        try {
            cc.testStarted();
        } catch (AuthenticationException e) {
            exeptionCaught = true;
        }
        assertTrue(exeptionCaught, "AuthenticationException did not occur.");
    }
}
