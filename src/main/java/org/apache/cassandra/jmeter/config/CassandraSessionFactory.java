package org.apache.cassandra.jmeter.config;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;

import java.util.HashMap;
import java.util.Map;

/**
 * DataStax Academy Sample Application
 * <p/>
 * Copyright 2013 DataStax
 */
public class CassandraSessionFactory {

  // This class supports both multiple cluster objects for different clusters, as well as
  // multiple sessions to the same cluster.

  // This does not support multiple cluster objects to the same host that differ in
  // parameters

  // TODO - When do we shut down a session or cluster??

  static CassandraSessionFactory instance;
  final Map<String, Cluster> clusters = new HashMap<String, Cluster>();
  final Map<String, Session> sessions = new HashMap<String, Session>();

  private void CassandraSessionFactory() {

  }


  public static synchronized CassandraSessionFactory getInstance() {
    if(instance == null) {
      instance = new CassandraSessionFactory();
    }
    return instance;
  }

  public static synchronized Session createSession(String host, String keyspace, LoadBalancingPolicy loadBalancingPolicy) {

    String sessionKey = host + keyspace;
    instance = getInstance();
    Session session = instance.sessions.get(sessionKey);
    if (session == null) {
        Cluster cluster = instance.clusters.get(host);

        if (cluster == null) {
           Cluster.Builder cb = Cluster.builder()
                    .addContactPoints(host)
                    .withReconnectionPolicy(new ConstantReconnectionPolicy(10000)) ;

            if (loadBalancingPolicy != null ) {
                cb = cb.withLoadBalancingPolicy(loadBalancingPolicy);
            }

            cluster = cb.build();

            instance.clusters.put(host, cluster);
        }

      if (keyspace != null && !keyspace.isEmpty())
        session = cluster.connect(keyspace);
      else
        session = cluster.connect();

        instance.sessions.put(sessionKey, session);
    }
    return session;
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
