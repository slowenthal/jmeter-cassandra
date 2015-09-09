package org.apache.cassandra.jmeter;/*
 *      Copyright (C) 2012 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

import com.datastax.driver.core.Row;
import org.apache.cassandra.jmeter.config.CassandraConnection;
import org.apache.cassandra.jmeter.sampler.CassandraSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testbeans.TestBeanHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Tests DataType class to ensure data sent in is the same as data received
 * All tests are executed via a Simple Statements
 * Counters are the only datatype not tested within the entirety of the suite.
 *     There is, however, an isolated test case that needs to be implemented.
 * All statements and sample data is easily exportable via the print_*() methods.
 */
public class MapTest extends JMeterTest {

    Logger logger = LoggerFactory.getLogger(MapTest.class);


    private static final String EXPECTED = "{1: 'one', 2:'two', 3:'three'}";
    private static final String TABLE = "maps";
    CassandraConnection cc = null;
    private static final String KEYSPACE = "k1";
    public static final String TESTSESSION = "testsession";

    @BeforeClass
    public void beforeClass() {
        super.beforeClass();

        session.execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':1}");
        session.execute("CREATE TABLE " + KEYSPACE + "." + TABLE + " (key int primary key, themap map<int, text> ) ");
        session.execute("insert into " + KEYSPACE + "." + TABLE + " (key, themap) values (1, " + EXPECTED + ")");

        // Create a cassandra connection
        cc = new CassandraConnection();
        cc.setProperty("contactPoints", NODE_1_IP);
        cc.setProperty("keyspace", KEYSPACE);
        cc.setProperty("sessionName", TESTSESSION);
        cc.testStarted();
    }

    @Test
    public void testSelectthemap() {
        CassandraSampler cs = new CassandraSampler();

        cs.setProperty("sessionName",TESTSESSION);
        cs.setProperty("consistencyLevel", AbstractCassandaTestElement.ONE);
        cs.setProperty("resultVariable","rv");
        cs.setProperty("queryType", AbstractCassandaTestElement.SIMPLE);
        cs.setProperty("query", "SELECT * FROM " + TABLE + " WHERE key = 1");

        TestBeanHelper.prepare(cs);

        SampleResult res = cs.sample(new Entry());
        assertTrue(res.isSuccessful(), res.getResponseMessage());

        String rowdata = new String(res.getResponseData());
        logger.info(rowdata);
        assertEquals(rowdata, "key\tthemap\n" + "1" + "\t" + "{1:one,2:two,3:three}" + "\n");
    }

    @Test
    public void testInsertPreparedthemap() {
        CassandraSampler cs = new CassandraSampler();

        cs.setProperty("sessionName",TESTSESSION);
        cs.setProperty("consistencyLevel", AbstractCassandaTestElement.ONE);
        cs.setProperty("resultVariable","rv");
        cs.setProperty("queryType", AbstractCassandaTestElement.PREPARED);
        cs.setProperty("queryArguments", "\"" + EXPECTED + "\"");
        cs.setProperty("query", "INSERT INTO " + TABLE + " (key,themap) VALUES (2, ?)");

        TestBeanHelper.prepare(cs);

        SampleResult res = cs.sample(new Entry());
        assertTrue(res.isSuccessful(), res.getResponseMessage());

        // See if the value matches
        Row row = session.execute("select themap from " + KEYSPACE + "." + TABLE + " where key = 2").one();
        Map<Integer, String> themap = row.getMap(0, Integer.class, String.class);
        assertTrue(themap.get(1).contentEquals("one"));
        assertTrue(themap.get(2).contentEquals("two"));
        assertTrue(themap.get(3).contentEquals("three"));
        assertEquals(3, themap.size());
    }
}
