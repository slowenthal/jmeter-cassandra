package org.apache.cassandra.jmeter;/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

import com.datastax.driver.core.*;
import org.apache.commons.collections.map.LRUMap;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.save.CSVSaveService;
import org.apache.jmeter.testelement.AbstractTestElement;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jmeter.threads.JMeterVariables;
import org.apache.jmeter.util.JMeterUtils;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A base class for all Cassandra test elements handling the basics of a CQL request.
 *
 */
public abstract class AbstractCassandaTestElement extends AbstractTestElement implements TestStateListener{
    private static final long serialVersionUID = 235L;

    private static final Logger log = LoggingManager.getLoggerForClass();

    private static final String COMMA = ","; // $NON-NLS-1$
    private static final char COMMA_CHAR = ',';

    private static final String UNDERSCORE = "_"; // $NON-NLS-1$

    // String used to indicate a null value
    private static final String NULL_MARKER =
            JMeterUtils.getPropDefault("cassandrasampler.nullmarker","]NULL["); // $NON-NLS-1$

    private static final int MAX_OPEN_PREPARED_STATEMENTS =
            JMeterUtils.getPropDefault("cassandrasampler.maxopenpreparedstatements", 100);

    protected static final String ENCODING = "UTF-8"; // $NON-NLS-1$

    // Query types (used to communicate with GUI)
    // N.B. These must not be changed, as they are used in the JMX files
    static final String SIMPLE   = "Simple Statement"; // $NON-NLS-1$
    static final String PREPARED = "Prepared Statement"; // $NON-NLS-1$

    public static final String CASSANDRA_DATE_FORMAT_STRING = "yyyy-MM-dd HH:mm:ssZ";
    public static final SimpleDateFormat CassandraDateFormat = new SimpleDateFormat(CASSANDRA_DATE_FORMAT_STRING);

    static final String ANY = "ANY";
    static final String ONE = "ONE";
    static final String TWO = "TWO";
    static final String THREE = "THREE";
    static final String QUORUM = "QUORUM";
    static final String ALL = "ALL";
    static final String LOCAL_ONE = "LOCAL_ONE";
    static final String LOCAL_QUORUM = "LOCAL_QUOURM";
    static final String EACH_QUORUM = "EACH_QUORUM";

    private String sessionName = ""; // $NON-NLS-1$
    private String queryArguments = ""; // $NON-NLS-1$
    private String variableNames = ""; // $NON-NLS-1$
    private String queryTimeout = ""; // $NON-NLS-1$
    private String queryType = "";
    private String consistencyLevel = ""; // $NON-NLS-1$
    private String query = ""; // $NON-NLS-1$


    private String resultVariable = ""; // $NON-NLS-1$

    /**
     *  Cache of PreparedStatements stored in a per-connection basis. Each entry of this
     *  cache is another Map mapping the statement string to the actual PreparedStatement.
     *  At one time a Connection is only held by one thread
     */
    private static final Map<Session, Map<String, PreparedStatement>> perConnCache =
            new ConcurrentHashMap<Session, Map<String, PreparedStatement>>();

    /**
     * Creates a CassandraSampler.
     */
    protected AbstractCassandaTestElement() {
    }

    /**
     * Execute the test element.
     *
     *
     * @param conn a {@link org.apache.jmeter.samplers.SampleResult} in case the test should sample; <code>null</code> if only execution is requested
     * @throws UnsupportedOperationException if the user provided incorrect query type
     */
    protected byte[] execute(Session conn) throws IOException {
        log.debug("executing cql");

        // Based on query return value, get results
        String _queryType = getQueryType();
        if (SIMPLE.equals(_queryType)) {
//       stmt.setQueryTimeout(getIntegerQueryTimeout());   // TODO - address this

            // TODO - set page size

            ResultSet rs = null;
            SimpleStatement stmt = new SimpleStatement(getQuery());
            stmt.setConsistencyLevel(getConsistencyLevelCL());
            rs = conn.execute(stmt);
            return getStringFromResultSet(rs).getBytes(ENCODING);

        } else if (PREPARED.equals(_queryType)) {
            BoundStatement pstmt = getPreparedStatement(conn);
            setArguments(pstmt);
            pstmt.setConsistencyLevel(getConsistencyLevelCL()) ;
            ResultSet rs = null;
            rs = conn.execute(pstmt);
            return getStringFromResultSet(rs).getBytes(ENCODING);
        } else { // User provided incorrect query type
            throw new UnsupportedOperationException("Unexpected query type: " + _queryType);
        }
    }

    private void setArguments(BoundStatement pstmt) throws IOException {
        if (getQueryArguments().trim().length()==0) {
            return;
        }

        ColumnDefinitions colDefs = pstmt.preparedStatement().getVariables();

        String[] arguments = CSVSaveService.csvSplitString(getQueryArguments(), COMMA_CHAR);
        if (arguments.length !=colDefs.size()) {
            throw new RuntimeException("number of arguments ("+arguments.length+") and number in stmt (" + colDefs.size() + ") are not equal");
        }


        for (int i = 0; i < arguments.length; i++) {
            String argument = arguments[i];


            Class<?> javaType = colDefs.getType(i).asJavaClass();
            try {
                if (javaType == Integer.class)
                    pstmt.setInt(i, Integer.parseInt(argument));
                else if (javaType == Boolean.class)
                    pstmt.setBool(i, Boolean.parseBoolean(argument));
                else if (javaType == ByteBuffer.class)  {
                    // TODO - figure this out byte buffer
                    throw new RuntimeException("Not yet implemented - byte buffer");
                }
                else if (javaType == Date.class)
                    pstmt.setDate(i,CassandraDateFormat.parse(argument));
                else if (javaType == BigDecimal.class)
                    pstmt.setDecimal(i, new BigDecimal(argument));
                else if (javaType == Double.class)
                    pstmt.setDouble(i, Double.parseDouble(argument));
                else if (javaType == Float.class)
                    pstmt.setFloat(i, Float.parseFloat(argument));
                else if (javaType == InetAddress.class)  {
                    // TODO - inet address
                    throw new RuntimeException("Not yet implemented - inet address");
                }
                else if (javaType == Long.class)
                    pstmt.setLong(i, Long.parseLong(argument));
                else if (javaType == String.class)
                    pstmt.setString(i, argument);
                else if (javaType == UUID.class)
                    pstmt.setUUID(i, UUID.fromString(argument));
                else if (javaType == BigInteger.class)
                    pstmt.setVarint(i, new BigInteger(argument));
                else
                    throw new RuntimeException("Unsupported Type: " + javaType);

            } catch (ParseException e) {
                throw new RuntimeException("Could not Convert Argument #" + i + " \"" + argument + "\" to type" + javaType) ;
            } catch (NullPointerException e) {
                throw new RuntimeException("Could not set argument no: "+(i+1)+" - missing parameter marker?");
            }
        }
    }

    private BoundStatement getPreparedStatement(Session conn) {
        return getPreparedStatement(conn,false);
    }

    private BoundStatement getPreparedStatement(Session conn, boolean callable) {
        Map<String, PreparedStatement> preparedStatementMap = perConnCache.get(conn);
        if (null == preparedStatementMap ) {
            @SuppressWarnings("unchecked") // LRUMap is not generic
                    Map<String, PreparedStatement> lruMap = new LRUMap(MAX_OPEN_PREPARED_STATEMENTS) {
                private static final long serialVersionUID = 1L;
                @Override
                protected boolean removeLRU(LinkEntry entry) {
                    PreparedStatement preparedStatement = (PreparedStatement)entry.getValue();
                    return true;
                }
            };
            preparedStatementMap = Collections.<String, PreparedStatement>synchronizedMap(lruMap);
            // As a connection is held by only one thread, we cannot already have a 
            // preparedStatementMap put by another thread
            perConnCache.put(conn, preparedStatementMap);
        }
        PreparedStatement pstmt = preparedStatementMap.get(getQuery());
        if (null == pstmt) {
            pstmt = conn.prepare(getQuery());

//            pstmt.setQueryTimeout(getIntegerQueryTimeout());
            // PreparedStatementMap is associated to one connection so 
            //  2 threads cannot use the same PreparedStatement map at the same time
            preparedStatementMap.put(getQuery(), pstmt);
        }

        // TODO - again, how do we handle timeouts?
//        else {
//            int timeoutInS = getIntegerQueryTimeout();
//            if(pstmt.getQueryTimeout() != timeoutInS) {
//                pstmt.setQueryTimeout(getIntegerQueryTimeout());
//            }
//        }

        return pstmt.bind();
    }

    private Object getObject ( Row row, int index ) {


        if (row.isNull(index))
            return null;


        DataType columnType = row.getColumnDefinitions().getType(index);
        if (columnType.isCollection()) {
            throw new RuntimeException("Implement me - collections");
        }

        Class<?> javaType = columnType.asJavaClass();


        if (javaType == Integer.class)
            return row.getInt(index);
        if (javaType == Boolean.class)
            return row.getBool(index);
        if (javaType == ByteBuffer.class)
            return row.getBytes(index);
        if (javaType == Date.class)
            return row.getDate(index);
        if (javaType == BigDecimal.class)
            return row.getDecimal(index);
        if (javaType == Double.class)
            return row.getDouble(index);
        if (javaType == Float.class)
            return row.getFloat(index);
        if (javaType == InetAddress.class)
            return row.getInet(index);
        if (javaType == Long.class)
            return row.getLong(index);
        if (javaType == String.class)
            return row.getString(index);
        if (javaType == UUID.class)
            return row.getUUID(index);
        if (javaType == BigInteger.class)
            return row.getVarint(index);


        throw new RuntimeException("Type "+ javaType + " is not supported");
    }


    /**
     * Gets a Data object from a ResultSet.
     *
     * @param rs
     *            ResultSet passed in from a database query
     * @return a Data object
     */

    private String getStringFromResultSet(ResultSet rs) throws UnsupportedEncodingException {

        ColumnDefinitions meta = rs.getColumnDefinitions();

        StringBuilder sb = new StringBuilder();

        int numColumns = rs.getColumnDefinitions().size();
        for (int i = 0; i < numColumns; i++) {
            sb.append(meta.getName(i));
            if (i==numColumns - 1){
                sb.append('\n');
            } else {
                sb.append('\t');
            }
        }

        JMeterVariables jmvars = getThreadContext().getVariables();
        String varnames[] = getVariableNames().split(COMMA);
        String resultVariable = getResultVariable().trim();
        List<Map<String, Object> > results = null;
        if(resultVariable.length() > 0) {
            results = new ArrayList<Map<String,Object> >();
            jmvars.putObject(resultVariable, results);
        }


        int j = 0;
        for (Row crow : rs) {
            Map<String, Object> row = null;
            j++;
            for (int i = 0; i < numColumns; i++) {

                Object o = getObject(crow,i) ;
                if(results != null) {
                    if(row == null) {
                        row = new HashMap<String, Object>(numColumns);
                        results.add(row);
                    }
                    row.put(rs.getColumnDefinitions().getName(i), o);
                }

// TODO - Do we need to do something with bytebuffers
//                if (rs.getColumnDefinitions().getType(i).asJavaClass() == byte[].class) {
//                    o = new String((byte[]) row.ge, ENCODING);
//                }

                sb.append(o);
                if (i==numColumns -1){
                    sb.append('\n');
                } else {
                    sb.append('\t');
                }
                if (i < varnames.length) { // i starts at 0
                    String name = varnames[i].trim();
                    if (name.length()>0){ // Save the value in the variable if present
                        jmvars.put(name+UNDERSCORE+j, o == null ? null : o.toString());
                    }
                }
            }
        }

        // Remove any additional values from previous sample
        for (String varname : varnames) {
            String name = varname.trim();
            if (name.length() > 0 && jmvars != null) {
                final String varCount = name + "_#"; // $NON-NLS-1$
                // Get the previous count
                String prevCount = jmvars.get(varCount);
                if (prevCount != null) {
                    int prev = Integer.parseInt(prevCount);
                    for (int n = j + 1; n <= prev; n++) {
                        jmvars.remove(name + UNDERSCORE + n);
                    }
                }
                jmvars.put(varCount, Integer.toString(j)); // save the current count
            }
        }

        return sb.toString();
    }

    public static void close(Session c) {

        // TODO - implement some sort of close
    }

    public static void close(Statement s) {
        // TODO - we probably don't need to do anything here
    }

    public static void close(ResultSet rs) {
        // TODO - again, probably no-op
    }

    /**
     * @return the integer representation queryTimeout
     */
    public int getIntegerQueryTimeout() {
        int timeout = 0;
        try {
            timeout = Integer.parseInt(queryTimeout);
        } catch (NumberFormatException nfe) {
            timeout = 0;
        }
        return timeout;
    }

    /**
     * @return the queryTimeout
     */
    public String getQueryTimeout() {
        return queryTimeout ;
    }

    /**
     * @param queryTimeout query timeout in seconds
     */
    public void setQueryTimeout(String queryTimeout) {
        this.queryTimeout = queryTimeout;
    }

    public String getQuery() {
        return query;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(80);
        sb.append("["); // $NON-NLS-1$
        sb.append(getQueryType());
        sb.append("] "); // $NON-NLS-1$
        sb.append(getQuery());
        sb.append("\n");
        sb.append(getQueryArguments());
        sb.append("\n");
        return sb.toString();
    }

    /**
     * @param query
     *            The query to set.
     */
    public void setQuery(String query) {
        this.query = query;
    }

    /**
     * @return Returns the dataSource.
     */
    public String getSessionName() {
        return sessionName;
    }

    /**
     * @param sessionName
     *            The dataSource to set.
     */
    public void setDataSource(String sessionName) {
        this.sessionName = sessionName;
    }

    /**
     * @return Returns the queryType.
     */
    public String getQueryType() {
        return queryType;
    }

    /**
     * @param queryType The queryType to set.
     */
    public void setQueryType(String queryType) {
        this.queryType = queryType;
    }

    public String getQueryArguments() {
        return queryArguments;
    }

    public void setQueryArguments(String queryArguments) {
        this.queryArguments = queryArguments;
    }

    /**
     * @return the variableNames
     */
    public String getVariableNames() {
        return variableNames;
    }

    /**
     * @param variableNames the variableNames to set
     */
    public void setVariableNames(String variableNames) {
        this.variableNames = variableNames;
    }

    /**
     * @return the resultVariable
     */
    public String getResultVariable() {
        return resultVariable ;
    }

    /**
     * @param resultVariable the variable name in which results will be stored
     */
    public void setResultVariable(String resultVariable) {
        this.resultVariable = resultVariable;
    }

    public String getConsistencyLevel() {
        return consistencyLevel;
    }

    public ConsistencyLevel getConsistencyLevelCL() {
        return ConsistencyLevel.valueOf(consistencyLevel);
    }

    public void setConsistencyLevel(String consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
    }

    public void setSessionName(String sessionName) {
        this.sessionName = sessionName;
    }

    /**
     * {@inheritDoc}
     * @see org.apache.jmeter.testelement.TestStateListener#testStarted()
     */
    @Override
    public void testStarted() {
        testStarted("");
    }

    /**
     * {@inheritDoc}
     * @see org.apache.jmeter.testelement.TestStateListener#testStarted(String)
     */
    @Override
    public void testStarted(String host) {
        cleanCache();
    }

    /**
     * {@inheritDoc}
     * @see org.apache.jmeter.testelement.TestStateListener#testEnded()
     */
    @Override
    public void testEnded() {
        testEnded("");
    }

    /**
     * {@inheritDoc}
     * @see org.apache.jmeter.testelement.TestStateListener#testEnded(String)
     */
    @Override
    public void testEnded(String host) {
        cleanCache();
    }

    /**
     * Clean cache of PreparedStatements
     */
    private static void cleanCache() {
        perConnCache.clear();
    }

}