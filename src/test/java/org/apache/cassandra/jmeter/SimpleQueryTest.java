package org.apache.cassandra.jmeter;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.cassandra.jmeter.config.CassandraConnection;
import org.apache.cassandra.jmeter.sampler.CassandraSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testbeans.TestBeanHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * DataStax Academy Sample Application
 * <p/>
 * Copyright 2013 DataStax
 */

public class SimpleQueryTest extends JMeterTest {

    public static final String TESTSESSION = "testsession";
    private static final String KEYSPACE = "k1";
    CassandraConnection cc = null;

    Logger logger = LoggerFactory.getLogger(SimpleQueryTest.class);

    @BeforeClass
    public void beforeClass() {
        super.beforeClass();

        // set up data. The table is of the form:
        // table <datatypename> (k <datatypename> primary key, v <datatypename))
        //
        session.execute("CREATE KEYSPACE " + KEYSPACE + " WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':1}");
        for (Object[] data : provideData()) {
            session.execute("CREATE TABLE " + KEYSPACE + "." + data[0] + " (k " + data[0] + " PRIMARY KEY, v " + data[0] + ")");
            session.execute(session.prepare("INSERT INTO " + KEYSPACE + "." + data[0] + " (k, v) VALUES (?, ?)").bind(data[2],data[2]));
        }

        // set up collection data
        for (final Object[] data : provideData()) {
            // sets
            session.execute("CREATE TABLE " + KEYSPACE + ".set_" + data[0] + " (k " + data[0] + " PRIMARY KEY, v SET<" + data[0] + ">)");
            session.execute(session.prepare("INSERT INTO " + KEYSPACE + ".set_" + data[0] + " (k, v) VALUES (?, ?)").bind(data[2], Sets.newHashSet(data[2])));

            // lists
            session.execute("CREATE TABLE " + KEYSPACE + ".list_" + data[0] + " (k " + data[0] + " PRIMARY KEY, v LIST<" + data[0] + ">)");
            session.execute(session.prepare("INSERT INTO " + KEYSPACE + ".list_" + data[0] + " (k, v) VALUES (?, ?)").bind(data[2], Lists.newArrayList(data[2])));

            // maps
            session.execute("CREATE TABLE " + KEYSPACE + ".map_" + data[0] + " (k " + data[0] + " PRIMARY KEY, v MAP<" + data[0] + ", "+ data[0] + ">)");
            session.execute(session.prepare("INSERT INTO " + KEYSPACE + ".map_" + data[0] + " (k, v) VALUES (?, ?)").bind(data[2],new HashMap<Object, Object>() {{ put(data[2], data[2]); }} ));
        }


        // Create a cassandra connection
        cc = new CassandraConnection();
        cc.setProperty("contactPoints", NODE_1_IP);
        cc.setProperty("keyspace", KEYSPACE);
        cc.setProperty("sessionName", TESTSESSION);
        cc.testStarted();
    }

    // Data for the tables
    @DataProvider(name = "provideQueries")
    public Object[][] provideData() {

        InetAddress ia = null;
        try {
            ia = InetAddress.getByName("123.123.123.123") ;
        } catch (UnknownHostException e) {
            fail("can't make inet address");
        }

        ByteBuffer bb = ByteBuffer.allocate(58);
        bb.putShort((short) 0xCAFE);
        bb.flip();


        return new Object[][] {
                // table name, string version, native version

                { "ascii", "ascii", "ascii" },
                { "bigint", "9223372036854775807", Long.MAX_VALUE },
                { "blob", "ascii", bb },
                { "boolean",  "true", Boolean.TRUE },
                { "decimal", "1.23E+8", new BigDecimal("12.3E+7") },
                { "double", "1.7976931348623157E308",  Double.MAX_VALUE},
                { "float", "3.4028235E38", Float.MAX_VALUE },
                { "inet", "/123.123.123.123", ia },
                { "int",  "2147483647", Integer.MAX_VALUE },
                { "text", "text", "text" },
                { "timestamp", "1997-08-28 23:14:00-0700", new Date(872835240000L) },
                { "timeuuid", "fe2b4360-28c6-11e2-81c1-0800200c9a66", UUID.fromString("FE2B4360-28C6-11E2-81C1-0800200C9A66") },
                { "uuid", "067e6162-3b6f-4ae2-a171-2470b63dff00", UUID.fromString("067e6162-3b6f-4ae2-a171-2470b63dff00") },
                { "varchar", "varchar", "varchar" },
                { "varint", "2147483647000", new BigInteger(Integer.toString(Integer.MAX_VALUE) + "000") }
        };
    }

    @Test(dataProvider = "provideQueries")
    public void testSimpleQuery(String table, String expected, Object nothing) {

        CassandraSampler cs = new CassandraSampler();
        cs.setProperty("sessionName",TESTSESSION);
        cs.setProperty("consistencyLevel", AbstractCassandaTestElement.ONE);
        cs.setProperty("resultVariable","rv");
        cs.setProperty("queryType", AbstractCassandaTestElement.SIMPLE);
        cs.setProperty("query", "SELECT * FROM " + table);
        TestBeanHelper.prepare(cs);

        SampleResult res = cs.sample(new Entry());
        assertTrue(res.isSuccessful(), res.getResponseMessage());

        String rowdata = new String(res.getResponseData());
        logger.info(rowdata);
        assertEquals(rowdata, "k\tv\n"+ expected +"\t"+ expected +"\n");
    }

    @Test(dataProvider = "provideQueries")
    public void testPreparedQuery(String table, String expected, Object nothing) {

        CassandraSampler cs = new CassandraSampler();
        cs.setProperty("sessionName",TESTSESSION);
        cs.setProperty("consistencyLevel", AbstractCassandaTestElement.ONE);
        cs.setProperty("resultVariable","rv");
        cs.setProperty("queryType", AbstractCassandaTestElement.PREPARED);
        cs.setProperty("queryArguments", expected );
        cs.setProperty("query", "SELECT * FROM " + table + " WHERE K = ?");
        TestBeanHelper.prepare(cs);

        SampleResult res = cs.sample(new Entry());
        assertTrue(res.isSuccessful(), res.getResponseMessage());

        String rowdata = new String(res.getResponseData());
        logger.debug(rowdata);
        assertEquals(rowdata, "k\tv\n"+ expected +"\t"+ expected +"\n");
    }

    @Test(dataProvider = "provideQueries")
    public void testPreparedSetQuery(String table, String expected, Object nothing) {

        CassandraSampler cs = new CassandraSampler();
        cs.setProperty("sessionName",TESTSESSION);
        cs.setProperty("consistencyLevel", AbstractCassandaTestElement.ONE);
        cs.setProperty("resultVariable","rv");
        cs.setProperty("queryType", AbstractCassandaTestElement.PREPARED);
        cs.setProperty("queryArguments", expected );
        cs.setProperty("query", "SELECT * FROM set_" + table + " WHERE K = ?");
        TestBeanHelper.prepare(cs);

        SampleResult res = cs.sample(new Entry());
        assertTrue(res.isSuccessful(), res.getResponseMessage());

        String rowdata = new String(res.getResponseData());
        logger.debug(rowdata);
        assertEquals(rowdata, "k\tv\n"+ expected +"\t"+ expected +"\n");
    }

    @Test(dataProvider = "provideQueries")
    public void testPreparedListQuery(String table, String expected, Object nothing) {

        CassandraSampler cs = new CassandraSampler();
        cs.setProperty("sessionName",TESTSESSION);
        cs.setProperty("consistencyLevel", AbstractCassandaTestElement.ONE);
        cs.setProperty("resultVariable","rv");
        cs.setProperty("queryType", AbstractCassandaTestElement.PREPARED);
        cs.setProperty("queryArguments", expected );
        cs.setProperty("query", "SELECT * FROM list_" + table + " WHERE K = ?");
        TestBeanHelper.prepare(cs);

        SampleResult res = cs.sample(new Entry());
        assertTrue(res.isSuccessful(), res.getResponseMessage());

        String rowdata = new String(res.getResponseData());
        logger.debug(rowdata);
        assertEquals(rowdata, "k\tv\n"+ expected +"\t"+ expected +"\n");
    }
    @Test(dataProvider = "provideQueries")
    public void testPreparedMapQuery(String table, String expected, Object nothing) {

        CassandraSampler cs = new CassandraSampler();
        cs.setProperty("sessionName",TESTSESSION);
        cs.setProperty("consistencyLevel", AbstractCassandaTestElement.ONE);
        cs.setProperty("resultVariable","rv");
        cs.setProperty("queryType", AbstractCassandaTestElement.PREPARED);
        cs.setProperty("queryArguments", expected );
        cs.setProperty("query", "SELECT * FROM map_" + table + " WHERE K = ?");
        TestBeanHelper.prepare(cs);

        SampleResult res = cs.sample(new Entry());
        assertTrue(res.isSuccessful(), res.getResponseMessage());

        String rowdata = new String(res.getResponseData());
        logger.debug(rowdata);
        assertEquals(rowdata, "k\tv\n"+ expected +"\t"+ expected +"\n");
    }
}
