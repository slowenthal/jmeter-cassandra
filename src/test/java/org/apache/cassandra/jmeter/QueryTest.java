package org.apache.cassandra.jmeter;

import org.apache.cassandra.jmeter.config.CassandraConnection;
import org.apache.cassandra.jmeter.sampler.CassandraSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testbeans.TestBeanHelper;
import org.apache.jmeter.threads.JMeterVariables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.*;
import static org.testng.Assert.*;


import static com.sun.jmx.snmp.ThreadContext.getThreadContext;

/**
 * DataStax Academy Sample Application
 * <p/>
 * Copyright 2013 DataStax
 */

public class QueryTest extends JMeterTest {

    public static final String TESTSESSION = "testsession";
    CassandraConnection cc = null;

    Logger logger = LoggerFactory.getLogger(QueryTest.class);

    @BeforeClass(groups = {"short", "long"})
    public void beforeClass() {
        super.beforeClass();

        cc = new CassandraConnection();

        cc.setProperty("contactPoints", NODE_1_IP);
        cc.setProperty("keyspace","ks");
        cc.setProperty("sessionName", TESTSESSION);
        cc.testStarted();
    }

    private CassandraSampler getBasicCassandraSampler() {
        CassandraSampler cs = new CassandraSampler();
        cs.setProperty("sessionName",TESTSESSION);
        cs.setProperty("consistencyLevel", AbstractCassandaTestElement.ONE);
        cs.setProperty("resultVariable","rv");
        cs.setProperty("queryType", AbstractCassandaTestElement.SIMPLE);
        TestBeanHelper.prepare(cs);
        return cs;
    }

    @Test
    public void testAsciiQuery() {

        CassandraSampler cs = getBasicCassandraSampler();
        cs.setQuery("select * from ascii");

        SampleResult res = cs.sample(new Entry());
        assertTrue(res.isSuccessful(), res.getResponseMessage());

        String rowdata = new String(res.getResponseData());
        logger.info(rowdata);
        assertEquals(rowdata, "k\tv\n" +
                "ascii\tascii\n");
    }

    @Test
    public void testBigintQuery() {

        CassandraSampler cs = getBasicCassandraSampler();
        cs.setQuery("select * from bigint");

        SampleResult res = cs.sample(new Entry());
        assertTrue(res.isSuccessful(), res.getResponseMessage());

        String rowdata = new String(res.getResponseData());
        logger.info(rowdata);
        assertEquals(rowdata, "k\tv\n" +
                "9223372036854775807\t9223372036854775807\n");
    }

    @Test
    public void testBlobQuery() {

        CassandraSampler cs = getBasicCassandraSampler();
        cs.setQuery("select * from blob");

        SampleResult res = cs.sample(new Entry());
        assertTrue(res.isSuccessful(), res.getResponseMessage());

        String rowdata = new String(res.getResponseData());
        logger.info(rowdata);
        assertEquals(rowdata, "k\tv\n" +
                "ascii\tascii");
    }

    @Test
    public void testBooleanQuery() {

        CassandraSampler cs = getBasicCassandraSampler();
        cs.setQuery("select * from boolean");

        SampleResult res = cs.sample(new Entry());
        assertTrue(res.isSuccessful(), res.getResponseMessage());

        String rowdata = new String(res.getResponseData());
        logger.info(rowdata);
        assertEquals(rowdata, "k\tv\n" +
                "true\ttrue\n");
    }

    @Test
    public void testDecimalQuery() {

        CassandraSampler cs = getBasicCassandraSampler();
        cs.setQuery("select * from decimal");

        SampleResult res = cs.sample(new Entry());
        assertTrue(res.isSuccessful(), res.getResponseMessage());

        String rowdata = new String(res.getResponseData());
        logger.info(rowdata);
        assertEquals(rowdata, "k\tv\n" +
                "1.23E+8\t1.23E+8\n");
    }

    @Test
    public void testDoubleQuery() {

        CassandraSampler cs = getBasicCassandraSampler();
        cs.setQuery("select * from double");

        SampleResult res = cs.sample(new Entry());
        assertTrue(res.isSuccessful(), res.getResponseMessage());

        String rowdata = new String(res.getResponseData());
        logger.info(rowdata);
        assertEquals(rowdata, "k\tv\n" +
                "1.7976931348623157E308\t1.7976931348623157E308\n");
    }

    @Test
    public void testFloatQuery() {

        CassandraSampler cs = getBasicCassandraSampler();
        cs.setQuery("select * from float");

        SampleResult res = cs.sample(new Entry());
        assertTrue(res.isSuccessful(), res.getResponseMessage());

        String rowdata = new String(res.getResponseData());
        logger.info(rowdata);
        assertEquals(rowdata, "k\tv\n" +
                "3.4028235E38\t3.4028235E38\n");
    }

    @Test
    public void testInetQuery() {

        CassandraSampler cs = getBasicCassandraSampler();
        cs.setQuery("select * from inet");

        SampleResult res = cs.sample(new Entry());
        assertTrue(res.isSuccessful(), res.getResponseMessage());

        String rowdata = new String(res.getResponseData());
        logger.info(rowdata);
        assertEquals(rowdata, "k\tv\n" +
                "/123.123.123.123\t/123.123.123.123\n");
    }

    @Test
    public void testIntQuery() {

        CassandraSampler cs = getBasicCassandraSampler();
        cs.setQuery("select * from int");

        SampleResult res = cs.sample(new Entry());
        assertTrue(res.isSuccessful(), res.getResponseMessage());

        String rowdata = new String(res.getResponseData());
        logger.info(rowdata);
        assertEquals(rowdata, "k\tv\n" +
                "2147483647\t2147483647\n");
    }

    @Test
    public void testTextQuery() {

        CassandraSampler cs = getBasicCassandraSampler();
        cs.setQuery("select * from text");

        SampleResult res = cs.sample(new Entry());
        assertTrue(res.isSuccessful(), res.getResponseMessage());

        String rowdata = new String(res.getResponseData());
        logger.info(rowdata);
        assertEquals(rowdata, "k\tv\n" +
                "text\ttext\n");
    }

    @Test
    public void testTimestampQuery() {

        CassandraSampler cs = getBasicCassandraSampler();
        cs.setQuery("select * from timestamp");

        SampleResult res = cs.sample(new Entry());
        assertTrue(res.isSuccessful(), res.getResponseMessage());

        String rowdata = new String(res.getResponseData());
        logger.info(rowdata);

        // TODO - this is time zone dependent!
        assertEquals(rowdata, "k\tv\n" +
                "1997-08-28 23:14:00-0700\t1997-08-28 23:14:00-0700\n");
    }

    @Test
    public void testTimeUUIDQuery() {

        CassandraSampler cs = getBasicCassandraSampler();
        cs.setQuery("select * from timeuuid");

        SampleResult res = cs.sample(new Entry());
        assertTrue(res.isSuccessful(), res.getResponseMessage());

        String rowdata = new String(res.getResponseData());
        logger.info(rowdata);
        assertEquals(rowdata, "k\tv\n" +
                "fe2b4360-28c6-11e2-81c1-0800200c9a66\tfe2b4360-28c6-11e2-81c1-0800200c9a66\n");
    }

    @Test
    public void testUUIDQuery() {

        CassandraSampler cs = getBasicCassandraSampler();
        cs.setQuery("select * from uuid");

        SampleResult res = cs.sample(new Entry());
        assertTrue(res.isSuccessful(), res.getResponseMessage());

        String rowdata = new String(res.getResponseData());
        logger.info(rowdata);
        assertEquals(rowdata, "k\tv\n" +
                "067e6162-3b6f-4ae2-a171-2470b63dff00\t067e6162-3b6f-4ae2-a171-2470b63dff00\n");
    }

    @Test
    public void testVarcharQuery() {

        CassandraSampler cs = getBasicCassandraSampler();
        cs.setQuery("select * from varchar");

        SampleResult res = cs.sample(new Entry());
        assertTrue(res.isSuccessful(), res.getResponseMessage());

        String rowdata = new String(res.getResponseData());
        logger.info(rowdata);
        assertEquals(rowdata, "k\tv\n" +
                "varchar\tvarchar\n");
    }

    @Test
    public void testVarintQuery() {

        CassandraSampler cs = getBasicCassandraSampler();
        cs.setQuery("select * from varint");

        SampleResult res = cs.sample(new Entry());
        assertTrue(res.isSuccessful(), res.getResponseMessage());

        String rowdata = new String(res.getResponseData());
        logger.info(rowdata);
        assertEquals(rowdata, "k\tv\n" +
                "2147483647000\t2147483647000\n");
    }
}
