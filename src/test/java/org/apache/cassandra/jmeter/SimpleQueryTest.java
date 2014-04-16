package org.apache.cassandra.jmeter;

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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * DataStax Academy Sample Application
 * <p/>
 * Copyright 2013 DataStax
 */

public class SimpleQueryTest extends JMeterTest {

    public static final String TESTSESSION = "testsession";
    CassandraConnection cc = null;

    Logger logger = LoggerFactory.getLogger(SimpleQueryTest.class);

    @BeforeClass(groups = {"short", "long"})
    public void beforeClass() {
        super.beforeClass();

        cc = new CassandraConnection();

        cc.setProperty("contactPoints", NODE_1_IP);
        cc.setProperty("keyspace","ks");
        cc.setProperty("sessionName", TESTSESSION);
        cc.testStarted();
    }


    @DataProvider(name = "provideQueries")
    public Object[][] provideData() {

        return new Object[][] {
                { "ascii", "ascii" },
                { "bigint", "9223372036854775807" },
                { "blob", "ascii" },
                { "boolean",  "true" },
                { "decimal", "1.23E+8" },
                { "double", "1.7976931348623157E308" },
                { "float", "3.4028235E38" },
                { "inet", "/123.123.123.123" },
                { "int",  "2147483647" },
                { "text", "text" },
                { "timestamp", "1997-08-28 23:14:00-0700" },
                { "timeuuid", "fe2b4360-28c6-11e2-81c1-0800200c9a66" },
                { "uuid", "067e6162-3b6f-4ae2-a171-2470b63dff00" },
                { "varchar", "varchar" },
                { "varint", "2147483647000" }
        };
    }

    @Test(dataProvider = "provideQueries")
    public void testSimpleQuery(String table, String expected) {

        CassandraSampler cs = new CassandraSampler();
        cs.setProperty("sessionName",TESTSESSION);
        cs.setProperty("consistencyLevel", AbstractCassandaTestElement.ONE);
        cs.setProperty("resultVariable","rv");
        cs.setProperty("queryType", AbstractCassandaTestElement.SIMPLE);
        cs.setProperty("query", "select * from " + table);
        TestBeanHelper.prepare(cs);

        SampleResult res = cs.sample(new Entry());
        assertTrue(res.isSuccessful(), res.getResponseMessage());

        String rowdata = new String(res.getResponseData());
        logger.info(rowdata);
        assertEquals(rowdata, "k\tv\n"+ expected +"\t"+ expected +"\n");
    }

    @Test(dataProvider = "provideQueries")
    public void testPreparedQuery(String table, String expected) {

        CassandraSampler cs = new CassandraSampler();
        cs.setProperty("sessionName",TESTSESSION);
        cs.setProperty("consistencyLevel", AbstractCassandaTestElement.ONE);
        cs.setProperty("resultVariable","rv");
        cs.setProperty("queryType", AbstractCassandaTestElement.PREPARED);
        cs.setProperty("queryArguments", expected );
        cs.setProperty("query", "select * from " + table + " where k = ?");
        TestBeanHelper.prepare(cs);

        SampleResult res = cs.sample(new Entry());
        assertTrue(res.isSuccessful(), res.getResponseMessage());

        String rowdata = new String(res.getResponseData());
        logger.info(rowdata);
        assertEquals(rowdata, "k\tv\n"+ expected +"\t"+ expected +"\n");
    }

}
