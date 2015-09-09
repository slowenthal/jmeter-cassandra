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

import org.apache.cassandra.jmeter.config.CassandraConnection;
import org.apache.cassandra.jmeter.sampler.CassandraSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testbeans.TestBeanHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Tests DataType class to ensure data sent in is the same as data received
 * All tests are executed via a Simple Statements
 * Counters are the only datatype not tested within the entirety of the suite.
 *     There is, however, an isolated test case that needs to be implemented.
 * All statements and sample data is easily exportable via the print_*() methods.
 */
public class UDTTest extends JMeterTest {

    Logger logger = LoggerFactory.getLogger(UDTTest.class);


    private static final String EXPECTED = "{t_int:10, t_string:'ten', t_float:10.0}";
    private static final String TABLE = "udt";
    CassandraConnection cc = null;
    private static final String KEYSPACE = "k1";
    public static final String TESTSESSION = "testsession";

    @BeforeClass
    public void beforeClass() {
        super.beforeClass();

        session.execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':1}");
        session.execute("CREATE TYPE IF NOT EXISTS " + KEYSPACE + ".NUMS_T (T_INT INT, T_STRING TEXT, T_FLOAT FLOAT)");
        session.execute("CREATE TABLE " + KEYSPACE + "." + TABLE + " (key int primary key, udt frozen<NUMS_T>) ");
        session.execute("insert into " + KEYSPACE + "." + TABLE + " (key, udt) values (1, " + EXPECTED + ")");

        // Create a cassandra connection
        cc = new CassandraConnection();
        cc.setProperty("contactPoints", NODE_1_IP);
        cc.setProperty("keyspace", KEYSPACE);
        cc.setProperty("sessionName", TESTSESSION);
        cc.testStarted();
    }

    @Test
    public void testSelectUDT() {
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
        assertEquals(rowdata, "key\tudt\n" + "1" + "\t" + EXPECTED + "\n");

    }

    @Test
    public void testInsertPreparedUDT() {
        CassandraSampler cs = new CassandraSampler();

        cs.setProperty("sessionName",TESTSESSION);
        cs.setProperty("consistencyLevel", AbstractCassandaTestElement.ONE);
        cs.setProperty("resultVariable","rv");
        cs.setProperty("queryType", AbstractCassandaTestElement.PREPARED);
        cs.setProperty("queryArguments", "\"" + EXPECTED + "\"");
        cs.setProperty("query", "INSERT INTO " + TABLE + " (key,udt) VALUES (2, ?)");

        TestBeanHelper.prepare(cs);

        SampleResult res = cs.sample(new Entry());
        assertTrue(res.isSuccessful(), res.getResponseMessage());

        // See if the value matches
        String val = session.execute("select udt from " + KEYSPACE + "." + TABLE + " where key = 2").one().getUDTValue(0).toString();
        assertEquals(val, EXPECTED);
    }
}
