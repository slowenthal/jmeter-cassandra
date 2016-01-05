package org.apache.cassandra.jmeter;
/*
 * Copyright 2014 Steven Lowenthal
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.datastax.driver.core.*;
import org.apache.commons.collections.map.LRUMap;
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
    static final String DYNAMIC_BATCH = "Dynamic Batch"; // $NON-NLS-1$

    public static final String CASSANDRA_DATE_FORMAT_STRING1 = "yyyy-MM-dd HH:mm:ssZ";
    public static final String CASSANDRA_DATE_FORMAT_STRING2 = "yyyy-MM-dd HH:mm:ss";
    public static final String CASSANDRA_DATE_FORMAT_STRING3 = "yyyy-MM-dd";
    public final SimpleDateFormat CassandraDateFormat1 = new SimpleDateFormat(CASSANDRA_DATE_FORMAT_STRING1);
    public final SimpleDateFormat CassandraDateFormat2 = new SimpleDateFormat(CASSANDRA_DATE_FORMAT_STRING2);
    public final SimpleDateFormat CassandraDateFormat3 = new SimpleDateFormat(CASSANDRA_DATE_FORMAT_STRING3);

    static final String ANY = "ANY";
    static final String ONE = "ONE";
    static final String TWO = "TWO";
    static final String THREE = "THREE";
    static final String QUORUM = "QUORUM";
    static final String ALL = "ALL";
    static final String LOCAL_ONE = "LOCAL_ONE";
    static final String LOCAL_QUORUM = "LOCAL_QUORUM";
    static final String EACH_QUORUM = "EACH_QUORUM";

    private String sessionName = ""; // $NON-NLS-1$
    private String queryArguments = ""; // $NON-NLS-1$
    private String variableNames = ""; // $NON-NLS-1$
    private String queryType = "";
    private String consistencyLevel = ""; // $NON-NLS-1$
    private String query = ""; // $NON-NLS-1$
    private Integer batchSize = 1;

    private String resultVariable = ""; // $NON-NLS-1$
    private transient final BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);  // TODO - needs to be a map with stmt name
    private int batchStatmentCount = 0;

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
        ResultSet rs = null;
        Statement stmt = null;
        if (SIMPLE.equals(_queryType)) {

            // TODO - set page size

            SimpleStatement sstmt = new SimpleStatement(getQuery());
            sstmt.setConsistencyLevel(getConsistencyLevelCL());
            stmt = sstmt;

        } else if (PREPARED.equals(_queryType) || DYNAMIC_BATCH.equals(_queryType)) {
            BoundStatement pstmt = getPreparedStatement(conn);
            setArguments(pstmt);
            pstmt.setConsistencyLevel(getConsistencyLevelCL()) ;
            stmt = pstmt;
            if (DYNAMIC_BATCH.equals(_queryType)) {
                BatchStatement batchStatement = this.batchStatement;  // TODO - replace this.batchstatement with a cache
                batchStatement.add(pstmt);
                if (++batchStatmentCount < batchSize)
                    return null;
                // Not too oo, but bail if we don't need to execute the batch
                stmt = batchStatement;
            }
        } else { // User provided incorrect query type
            throw new UnsupportedOperationException("Unexpected query type: " + _queryType);
        }
        batchStatmentCount = 0;
        // TODO - clean up setConsistencyLevel everywhere
        // TODO - This is the one that will always work
        stmt.setConsistencyLevel(getConsistencyLevelCL());
        rs = conn.execute(stmt);
        batchStatement.clear();   // You've got to be kidding!
        return getStringFromResultSet(rs).getBytes(ENCODING);
    }

    private static byte[] hexStringToByteArray(String s) throws ParseException {

        if (! s.startsWith("0x")) {
            throw new ParseException("blob must start with 0x", 0);
        }
        int len = s.length() -2 ;
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((charToHexDigit(s.charAt(i + 2)) << 4)
                    + charToHexDigit(s.charAt(i + 3)));
        }
        return data;
    }

    private static int charToHexDigit(char ch) throws ParseException {
        int digit = Character.digit(ch,16);
        if (digit == -1 ) {
            throw new ParseException("\"" + ch + "\" is an invalid character", 0);
        }
        return digit;
    }

    final protected static char[] hexArray = "0123456789abcdef".toCharArray();
    private static String bytesToHex(ByteBuffer bb) {
        char[] hexChars = new char[bb.remaining() * 2];
        int j=0;
        while (bb.hasRemaining() ) {
            int v = bb.get() & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
            j++;
        }

        return "0x" + new String(hexChars);
    }

    private void setArguments(BoundStatement pstmt) throws IOException {
        if (getQueryArguments().trim().length()==0) {
            return;
        }

        ColumnDefinitions colDefs = pstmt.preparedStatement().getVariables();

        String[] arguments = CSVSaveService.csvSplitString(getQueryArguments(), COMMA_CHAR);
        if (arguments.length !=colDefs.size()) {
            // TODO - throw a non-transient exception here!
            throw new RuntimeException("number of arguments ("+arguments.length+") and number in stmt (" + colDefs.size() + ") are not equal");
        }


        for (int i = 0; i < arguments.length; i++) {
            String argument = arguments[i];

            DataType tp = colDefs.getType(i);
            Class<?> javaType = tp.asJavaClass();
            try {
                if (javaType == Integer.class)
                    pstmt.setInt(i, Integer.parseInt(argument));
                else if (javaType == Boolean.class)
                    pstmt.setBool(i, Boolean.parseBoolean(argument));
                else if (javaType == ByteBuffer.class)
                    pstmt.setBytes(i, ByteBuffer.wrap(hexStringToByteArray(argument)));
                else if (javaType == Date.class) {
                    if (argument.length() == CASSANDRA_DATE_FORMAT_STRING1.length())
                        pstmt.setDate(i, CassandraDateFormat1.parse(argument));
                    else if (argument.length() == CASSANDRA_DATE_FORMAT_STRING2.length())
                        pstmt.setDate(i, CassandraDateFormat2.parse(argument));
                    else if (argument.length() == CASSANDRA_DATE_FORMAT_STRING3.length())
                        pstmt.setDate(i, CassandraDateFormat3.parse(argument));
                    }

                else if (javaType == BigDecimal.class)
                        pstmt.setDecimal(i, new BigDecimal(argument));
                else if (javaType == Double.class)
                        pstmt.setDouble(i, Double.parseDouble(argument));
                else if (javaType == Float.class)
                    pstmt.setFloat(i, Float.parseFloat(argument));
                else if (javaType == InetAddress.class)  {
                    int start = argument.startsWith("/") ? 1 : 0;    // strip off leading /
                        pstmt.setInet(i, InetAddress.getByName(argument.substring(start)));
                }
                else if (javaType == Long.class)
                    pstmt.setLong(i, Long.parseLong(argument));
                else if (javaType == String.class)
                    pstmt.setString(i, argument);
                else if (javaType == UUID.class)
                    pstmt.setUUID(i, UUID.fromString(argument));
                else if (javaType == BigInteger.class)
                    pstmt.setVarint(i, new BigInteger(argument));
                    else if (javaType == TupleValue.class) {
                    TupleValue tup = (TupleValue) tp.parse(argument);
                    pstmt.setTupleValue(i, tup);
                } else if (javaType == UDTValue.class) {
                    UDTValue udt = (UDTValue) tp.parse(argument);
                    pstmt.setUDTValue(i, udt);
                    } else if (javaType.isAssignableFrom(Set.class)) {
                    Set<?> theSet = (Set<?>) tp.parse(argument);
                    pstmt.setSet(i,theSet);
                } else if (javaType.isAssignableFrom(List.class)) {
                    List<?> theList = (List<?>) tp.parse(argument);
                    pstmt.setList(i,theList);
                } else if (javaType.isAssignableFrom(Map.class)) {
                    Map<?,?> theMap = (Map<?,?>) tp.parse(argument);
                    pstmt.setMap(i,theMap);
                }
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

    // TODO - How thread safe is this - conn gets shared for everyone.
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

            // TODO - This is wrong, but is synchronized
            preparedStatementMap = Collections.<String, PreparedStatement>synchronizedMap(lruMap);
            // As a connection is held by only one thread, we cannot already have a 
            // preparedStatementMap put by another thread
            perConnCache.put(conn, preparedStatementMap);
        }
        PreparedStatement pstmt = preparedStatementMap.get(getQuery());
        if (null == pstmt) {
            pstmt = conn.prepare(getQuery());

            // PreparedStatementMap is associated to one connection so
            //  2 threads cannot use the same PreparedStatement map at the same time
            preparedStatementMap.put(getQuery(), pstmt);
        }

        return pstmt.bind();
    }

    private String stringOf(Object o) {
       if (o.getClass() == Date.class)
           return CassandraDateFormat1.format(o);
       else if (ByteBuffer.class.isAssignableFrom(o.getClass()))
           return bytesToHex((ByteBuffer) o);
       else
           return o.toString();
    }

    private Object getObject ( Row row, int index ) {


        if (row.isNull(index))
            return null;


        DataType columnType = row.getColumnDefinitions().getType(index);
        if (columnType.isCollection()) {
            if (columnType.asJavaClass().isAssignableFrom(Set.class)) {
                Class<?> innerType = columnType.getTypeArguments().get(0).asJavaClass();
                StringBuilder sb = new StringBuilder("{");
                String comma = "";
                for (Object o : row.getSet(index,innerType)) {
                    sb.append(comma).append(stringOf(o));
                    comma=",";
                }
                sb.append("}");
                return sb;
            }
            if (columnType.asJavaClass().isAssignableFrom(List.class)) {
                Class<?> innerType = columnType.getTypeArguments().get(0).asJavaClass();
                StringBuilder sb = new StringBuilder("[");
                String comma = "";
                for (Object o : row.getList(index, innerType)) {
                    sb.append(comma).append(stringOf(o));
                    comma=",";
                }
                sb.append("]");
                return sb;
            }
            if (columnType.asJavaClass().isAssignableFrom(Map.class)) {
                Class<?> keyType = columnType.getTypeArguments().get(0).asJavaClass();
                Class<?> valueType = columnType.getTypeArguments().get(1).asJavaClass();
                StringBuilder sb = new StringBuilder("{");
                String comma = "";
                for (Map.Entry<?,?> e :  row.getMap(index, keyType, valueType).entrySet()) {
                    sb.append(comma)
                      .append(stringOf(e.getKey()))
                      .append(':')
                      .append(stringOf(e.getValue()));
                    comma=",";
                }
                sb.append("}");
                return sb;
            }
            throw new RuntimeException("Unknown collection type: " + columnType.getName() );
        }

        Class<?> javaType = columnType.asJavaClass();

        if (javaType == Integer.class)
            return row.getInt(index);
        if (javaType == Boolean.class)
            return row.getBool(index);
        if (javaType == ByteBuffer.class)
            return row.getBytes(index);
        if (javaType == Date.class)
            return CassandraDateFormat1.format(row.getDate(index));
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
        if (javaType == TupleValue.class)
            return row.getTupleValue(index);
        if (javaType == UDTValue.class)
            return row.getUDTValue(index);


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

                if (rs.getColumnDefinitions().getType(i).asJavaClass() == ByteBuffer.class) {
                    o = bytesToHex((ByteBuffer) o);
                }

                if(results != null) {
                    if(row == null) {
                        row = new HashMap<String, Object>(numColumns);
                        results.add(row);
                    }
                    row.put(rs.getColumnDefinitions().getName(i), o);
                }

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
        int x=1;
        // TODO - implement some sort of close
    }

    public static void close(Statement s) {
        int x=1;
        // TODO - we probably don't need to do anything here
        // TODO - submit any open batches
    }

    public static void close(ResultSet rs) {
        int x=1;
        // TODO - again, probably no-op
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

    public String getBatchSize() {
        return batchSize.toString();
    }

    public void setBatchSize(String batchSize) {
        try {
            this.batchSize = Integer.parseInt(batchSize);
        } catch (NumberFormatException e) {
            // Not the most kosher thing to do, but prevents annoying exception handling
            this.batchSize = 1;
        }
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
    public void testStarted() {
        testStarted("");
    }

    /**
     * {@inheritDoc}
     * @see org.apache.jmeter.testelement.TestStateListener#testStarted(String)
     */
    public void testStarted(String host) {
        cleanCache();
    }

    /**
     * {@inheritDoc}
     * @see org.apache.jmeter.testelement.TestStateListener#testEnded()
     */
    public void testEnded() {
        testEnded("");
    }

    /**
     * {@inheritDoc}
     * @see org.apache.jmeter.testelement.TestStateListener#testEnded(String)
     */
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