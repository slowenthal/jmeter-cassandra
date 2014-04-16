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

    @Override
    protected Collection<String> getTableDefinitions() {
        // We do our own setup in the tests
        // send an empty array
        return new ArrayList<String>();
    }

    public void beforeClass() {
        super.beforeClass();
    }

    // TODO - userid / password tests
    // TODO - multi-connection test
    // TODO - multi-session test

}
