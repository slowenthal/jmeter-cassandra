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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class CassandraSessionFactory {

  // This class supports both multiple cluster objects for different clusters, as well as
  // multiple sessions to the same cluster.

  // This does not support multiple cluster objects to the same host that differ in
  // parameters

  // TODO - When do we shut down a session or cluster??

  static CassandraSessionFactory instance;
  final Map<String, Session> sessions = new HashMap<String, Session>();

  private void CassandraSessionFactory() {

  }


  public static synchronized CassandraSessionFactory getInstance() {
    if(instance == null) {
      instance = new CassandraSessionFactory();
    }
    return instance;
  }

  public static synchronized Session createSession(String sessionKey, Set<InetAddress> host, String keyspace, String username, String password, LoadBalancingPolicy loadBalancingPolicy) {

    instance = getInstance();
    Session session = instance.sessions.get(sessionKey);
      if (session == null) {

          Cluster.Builder cb = Cluster.builder()
                  .addContactPoints(host)
                  .withReconnectionPolicy(new ConstantReconnectionPolicy(10000)) ;

          if (loadBalancingPolicy != null ) {
              cb = cb.withLoadBalancingPolicy(loadBalancingPolicy);
          }

          if ( username != null && ! username.isEmpty()) {
              cb = cb.withCredentials(username, password);
          }

          Cluster cluster = cb.build();


          if (keyspace != null && !keyspace.isEmpty())
        session = cluster.connect(keyspace);
      else
        session = cluster.connect();

        instance.sessions.put(sessionKey, session);
    }
    return session;
  }

  public static synchronized void destroyClusters() {
      for (Session session : instance.sessions.values()) {
          Cluster cluster = session.getCluster();
          session.close();
          cluster.close();
      }
      instance.sessions.clear();
  }

  public static synchronized void closeSession(Session session) {

      // Find the session
      for (Map.Entry<String, Session> entry : instance.sessions.entrySet()) {
           if (entry.getValue() == session) {
               session.close();
               instance.sessions.remove(entry.getKey());
               return;
           }
      }

      assert false: "Closing session that is not found";
  }

}
