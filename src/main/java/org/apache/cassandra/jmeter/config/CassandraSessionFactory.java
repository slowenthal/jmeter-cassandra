package org.apache.cassandra.jmeter.config;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;

/**
 * DataStax Academy Sample Application
 * <p/>
 * Copyright 2013 DataStax
 */
public class CassandraSessionFactory {

  static CassandraSessionFactory instance;
  Cluster cluster = null;
  Session session = null;

  private void CassandraSessionFactory() {

  }

  public static synchronized CassandraSessionFactory getInstance() {
    if(instance == null) {
      instance = new CassandraSessionFactory();
    }
    return instance;
  }

  public static synchronized Session getSession(String host, String keyspace) {

    instance = getInstance();
    if (instance.session == null) {
    Cluster cluster = Cluster.builder()
            .addContactPoints(host)
//            .withLoadBalancingPolicy(new LatencyAwarePolicy.Builder(new RoundRobinPolicy()).build())
            .withReconnectionPolicy(new ConstantReconnectionPolicy(10000))
            .build();

      instance.cluster = cluster;
      if (keyspace != null)
        instance.session = cluster.connect(keyspace);
      else
        instance.session = cluster.connect();

    }
    return instance.session;
  }

}
