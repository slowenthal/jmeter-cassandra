package org.apache.cassandra.jmeter;

import com.datastax.driver.core.CCMBridge;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Session;
import org.apache.cassandra.jmeter.config.CassandraConnection;
import org.apache.cassandra.jmeter.config.CassandraSessionFactory;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.threads.JMeterVariables;
import org.apache.jmeter.util.JMeterUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.charset.Charset;
import java.util.*;

import static org.testng.Assert.*;

/**
 * DataStax Academy Sample Application
 * <p/>
 * Copyright 2013 DataStax
 */


public class SessionFactoryTest extends CCMBridge.PerClassSingleNodeCluster {

    private final static Set<DataType> DATA_TYPE_PRIMITIVES = DataType.allPrimitiveTypes();
    public static final String NODE_1_IP = "127.0.1.1";
    public static final String SELECT_CLUSTER_NAME = "select cluster_name from system.local where key ='local'";


    private static boolean exclude(DataType t) {
        return t.getName() == DataType.Name.COUNTER;
    }

    @Override
    protected Collection<String> getTableDefinitions() {
        ArrayList<String> tableDefinitions = new ArrayList<String>();

        // Create primitive data type definitions
        for (DataType dataType : DATA_TYPE_PRIMITIVES) {
            if (exclude(dataType))
                continue;

            tableDefinitions.add(String.format("CREATE TABLE %1$s (k %2$s PRIMARY KEY, v %1$s)", dataType, dataType));
        }

        return tableDefinitions;
    }

    @BeforeClass
    public void beforeClass() {
        super.beforeClass();
    }

    @Test
    public void testConnectionNoKSNoLB() {

        Session session = CassandraSessionFactory.createSession("testsession",NODE_1_IP,null,null,null,null);

        assertNotNull(session);

        // check that we can select from system.local
        String clusterName = session.execute(SELECT_CLUSTER_NAME).one().getString(0);

        assertEquals(clusterName,"test");

        CassandraSessionFactory.closeSession(session);

        // Are we closed?
        assertTrue(session.isClosed(), "Session is Closed");

    }

    @Test
    public void testSecondConnection() {

        Session session = CassandraSessionFactory.createSession("testsession",NODE_1_IP,null,null,null,null);

        assertNotNull(session);

        // check that we can select from system.local
        String clusterName = session.execute(SELECT_CLUSTER_NAME).one().getString(0);

        assertEquals(clusterName,"test");

        Session session2 = CassandraSessionFactory.createSession("testsession2", NODE_1_IP,null,null,null,null);

        // Did we get back the same session?
        assertEquals(session, session2);

        CassandraSessionFactory.closeSession(session);

        // Are we closed?
        assertTrue(session.isClosed(), "Session is Closed");

        // Should assert
        try {
            CassandraSessionFactory.closeSession(session2);
        } catch (AssertionError e) {
            // assertion - ok
            return;
        }

        fail("closeSession did not assert");

    }

    // TODO test multiple sessions - same cluster
    // TODO test multiple sessions - different cluster
    // TODO test multi-node cluster
    // TODO duplicate cluster ??


}
