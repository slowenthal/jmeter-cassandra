package org.apache.cassandra.jmeter.config;
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

import org.apache.jmeter.testbeans.BeanInfoSupport;
import org.apache.jmeter.testbeans.gui.TypeEditor;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import java.beans.PropertyDescriptor;

public class CassandraConnectionBeanInfo extends BeanInfoSupport {
    private static final Logger log = LoggingManager.getLoggerForClass();

    public CassandraConnectionBeanInfo() {
        super(CassandraConnection.class);
    
        createPropertyGroup("varName", new String[] { "sessionName" });

        createPropertyGroup("cluster", new String[] { "contactPoints", "keyspace", "username", "password", "useSSL" });

        createPropertyGroup("loadbalancergroup", new String[]{"loadBalancer", "localDataCenter"});

        PropertyDescriptor p = property("contactPoints");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");
        p = property("sessionName");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");
        p = property("keyspace");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");
        p = property("username");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");
        p = property("password", TypeEditor.PasswordEditor);
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");

        p = property("loadBalancer"); // $NON-NLS-1$
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, CassandraConnection.DEFAULTLOADBALANCER);
        p.setValue(NOT_OTHER,Boolean.TRUE);
        p.setValue(TAGS,new String[]{
                 CassandraConnection.ROUND_ROBIN,
                 CassandraConnection.DC_AWARE_ROUND_ROBIN,
                 CassandraConnection.DC_TOKEN_AWARE,
                 CassandraConnection.WHITELIST,
                 CassandraConnection.DEFAULTLOADBALANCER
        });

        p = property("localDataCenter");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");

        p = property("useSSL");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(NOT_OTHER,Boolean.TRUE);
        p.setValue(TAGS,new String[]{
                CassandraConnection.FALSE,
                CassandraConnection.TRUE,
                });
        p.setValue(DEFAULT, CassandraConnection.FALSE);
    }
}
