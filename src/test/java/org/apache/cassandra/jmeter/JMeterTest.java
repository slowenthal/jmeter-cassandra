package org.apache.cassandra.jmeter;

import com.datastax.driver.core.CCMBridge;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.apache.cassandra.jmeter.config.CassandraConnection;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.threads.JMeterVariables;
import org.apache.jmeter.util.JMeterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * DataStax Academy Sample Application
 * <p/>
 * Copyright 2013 DataStax
 */


public class JMeterTest extends CCMBridge.PerClassSingleNodeCluster {

    private static final Logger logger = LoggerFactory.getLogger(JMeterTest.class);
    private final static Set<DataType> DATA_TYPE_PRIMITIVES = DataType.allPrimitiveTypes();
    private final static HashMap<DataType, Object> SAMPLE_DATA = getSampleData();
    private final static Collection<String> PRIMITIVE_INSERT_STATEMENTS = getPrimitiveInsertStatements();
    private final static String PRIMITIVE_INSERT_FORMAT = "INSERT INTO %1$s (k, v) VALUES (%2$s, %2$s);";


    private static final String filePrefix;
    public static final String NODE_1_IP = "127.0.1.1";

    private static void logprop(String prop) {
        logger.info(prop + "=" + System.getProperty(prop));
    }

/*
 * If not running under AllTests.java, make sure that the properties (and
 * log file) are set up correctly.
 *
 * N.B. In order for this to work correctly, the JUnit test must be started
 * in the bin directory, and all the JMeter jars (plus any others needed at
 * run-time) need to be on the classpath.
 *
 */
    static {
        if (JMeterUtils.getJMeterProperties() == null) {
            String file = "src/test/testfiles/jmetertest.properties";
            File f = new File(file);
            if (!f.canRead()) {
                System.out.println("Can't find " + file + " - trying bin directory");
                file = "bin/" + file;// JMeterUtils assumes Unix-style separators
                filePrefix = "bin/";
            } else {
                filePrefix = "";
            }
            // Used to be done in initializeProperties
            String home=new File(System.getProperty("user.dir"),filePrefix).getParent();
            System.out.println("Setting JMeterHome: "+home);
            JMeterUtils.setJMeterHome(home);
            System.setProperty("jmeter.home", home); // needed for scripts

            JMeterVariables vars = new JMeterVariables();
            JMeterContextService.getContext().setVariables(vars);

            JMeterUtils jmu = new JMeterUtils();
            try {
                jmu.initializeProperties(file);
            } catch (MissingResourceException e) {
                System.out.println("** Can't find resources - continuing anyway **");
            }
            System.out.println("JMeterVersion="+JMeterUtils.getJMeterVersion());
            logprop("java.version");
            logprop("java.vm.name");
            logprop("java.vendor");
            logprop("java.home");
            logprop("file.encoding");
            // Display actual encoding used (will differ if file.encoding is not recognised)
            System.out.println("default encoding="+ Charset.defaultCharset());
            logprop("user.home");
            logprop("user.dir");
            logprop("user.language");
            logprop("user.region");
            logprop("user.country");
            logprop("user.variant");
            System.out.println("Locale="+ Locale.getDefault().toString());
            logprop("java.class.version");
            logprop("os.name");
            logprop("os.version");
            logprop("os.arch");
            logprop("java.class.path");
            // String cp = System.getProperty("java.class.path");
            // String cpe[]= JOrphanUtils.split(cp,File.pathSeparator);
            // System.out.println("java.class.path=");
            // for (int i=0;i<cpe.length;i++){
            // System.out.println(cpe[i]);
            // }
        } else {
            filePrefix = "";
        }
    }

    private static Collection<String> getPrimitiveInsertStatements() {
        ArrayList<String> insertStatements = new ArrayList<String>();

        for (DataType dataType : SAMPLE_DATA.keySet()) {
            String value = helperStringifiedData(dataType);
            insertStatements.add(String.format(PRIMITIVE_INSERT_FORMAT, dataType, value));
        }

        return insertStatements;
    }

    /**
     * Helper method to stringify SAMPLE_DATA for simple insert statements
     */
    private static String helperStringifiedData(DataType dataType) {
        String value = SAMPLE_DATA.get(dataType).toString();

        switch (dataType.getName()) {
            case BLOB:
                value = "0xCAFE";
                break;

            case INET:
                InetAddress v1 = (InetAddress) SAMPLE_DATA.get(dataType);
                value = String.format("'%s'", v1.getHostAddress());
                break;

            case TIMESTAMP:
                Date v2 = (Date) SAMPLE_DATA.get(dataType);
                value = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format(v2);
            case ASCII:
            case TEXT:
            case VARCHAR:
                value = String.format("'%s'", value);
                break;

            default:
                break;
        }

        return value;
    }


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

    /**
     * Generates the sample data that will be used in testing
     */
    private static HashMap<DataType, Object> getSampleData() {
        HashMap<DataType, Object> sampleData = new HashMap<DataType, Object>();

        for (DataType dataType : DATA_TYPE_PRIMITIVES) {
            switch (dataType.getName()) {
                case ASCII:
                    sampleData.put(dataType, new String("ascii"));
                    break;
                case BIGINT:
                    sampleData.put(dataType, Long.MAX_VALUE);
                    break;
                case BLOB:
                    ByteBuffer bb = ByteBuffer.allocate(58);
                    bb.putShort((short) 0xCAFE);
                    bb.flip();
                    sampleData.put(dataType, bb);
                    break;
                case BOOLEAN:
                    sampleData.put(dataType, Boolean.TRUE);
                    break;
                case COUNTER:
                    // Not supported in an insert statement
                    break;
                case DECIMAL:
                    sampleData.put(dataType, new BigDecimal("12.3E+7"));
                    break;
                case DOUBLE:
                    sampleData.put(dataType, Double.MAX_VALUE);
                    break;
                case FLOAT:
                    sampleData.put(dataType, Float.MAX_VALUE);
                    break;
                case INET:
                    try {
                        sampleData.put(dataType, InetAddress.getByName("123.123.123.123"));
                    } catch (java.net.UnknownHostException e) {}
                    break;
                case INT:
                    sampleData.put(dataType, Integer.MAX_VALUE);
                    break;
                case TEXT:
                    sampleData.put(dataType, new String("text"));
                    break;
                case TIMESTAMP:
                    sampleData.put(dataType, new Date(872835240000L));
                    break;
                case TIMEUUID:
                    sampleData.put(dataType, UUID.fromString("FE2B4360-28C6-11E2-81C1-0800200C9A66"));
                    break;
                case UUID:
                    sampleData.put(dataType, UUID.fromString("067e6162-3b6f-4ae2-a171-2470b63dff00"));
                    break;
                case VARCHAR:
                    sampleData.put(dataType, new String("varchar"));
                    break;
                case VARINT:
                    sampleData.put(dataType, new BigInteger(Integer.toString(Integer.MAX_VALUE) + "000"));
                    break;
                default:
                    throw new RuntimeException("Missing handling of " + dataType);
            }
        }

        return sampleData;
    }

    public void beforeClass() {
        super.beforeClass();

        // Insert sample data

        ResultSet rs;
        for (String execute_string : PRIMITIVE_INSERT_STATEMENTS) {
            rs = session.execute(execute_string);
            assertTrue(rs.isExhausted());
        }
    }

    // TODO - userid / password tests
    // TODO - multi-connection test
    // TODO - multi-session test

}
