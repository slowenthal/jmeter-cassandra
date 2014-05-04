package org.apache.cassandra.jmeter.processor;
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

import com.datastax.driver.core.Session;
import org.apache.cassandra.jmeter.AbstractCassandaTestElement;
import org.apache.cassandra.jmeter.config.CassandraConnection;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.jorphan.util.JOrphanUtils;
import org.apache.log.Logger;

import java.io.IOException;

/**
 * As pre- and post-processors essentially do the same this class provides the implementation.
 */
public abstract class AbstractCassandraProcessor extends AbstractCassandaTestElement {
    
    private static final Logger log = LoggingManager.getLoggerForClass();

    private static final long serialVersionUID = 232L;

    /**
     * Calls the native driver code to be executed.
     */
    protected void process() {
        Session conn = null;
        if(JOrphanUtils.isBlank(getSessionName())) {
            throw new IllegalArgumentException("Variable Name must not be null in "+getName());
        }
        try {
            conn = CassandraConnection.getSession(getSessionName());
            execute(conn);
        }  catch (IOException ex) {
            log.warn("IO Problem in  "+ getName() + ": " + ex.toString());
        } catch (UnsupportedOperationException ex) {
            log.warn("Execution Problem in "+ getName() + ": " + ex.toString());
        } finally {
            AbstractCassandaTestElement.close(conn);
        }
    }

}
