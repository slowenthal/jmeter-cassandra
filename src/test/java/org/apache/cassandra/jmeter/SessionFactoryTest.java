package org.apache.cassandra.jmeter;

import com.datastax.driver.core.CCMBridge;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Session;
import org.apache.cassandra.jmeter.config.CassandraConnection;
import org.apache.cassandra.jmeter.config.CassandraSessionFactory;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.threads.JMeterVariables;
import org.apache.jmeter.util.JMeterUtils;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.charset.Charset;
import java.util.*;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * DataStax Academy Sample Application
 * <p/>
 * Copyright 2013 DataStax
 */


public class SessionFactoryTest extends CCMBridge.PerClassSingleNodeCluster {

    private final static Set<DataType> DATA_TYPE_PRIMITIVES = DataType.allPrimitiveTypes();
    public static final String NODE_1_IP = "127.0.1.1";


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


    @Test
    public void testConnectionNoKSNoLB() {

        Session session = CassandraSessionFactory.createSession(NODE_1_IP,null,null);

        assertNotNull(session);

        // check that we can select from system.local
        String clusterName = session.execute("select cluster_name from system.local where key ='local'").one().getString(0);

        assertEquals(clusterName,"test");

    }

}
