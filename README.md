# Cassandra JMeter Plugin for Cassandra

---
A CQL3 native plugin for Cassandra 2.0 using the DataStax Java Driver for Apache Cassandra. The plugin is loosely based on the JDBC Plugin included with JMeter originally written by Ruben Laguna. It includes 4 components:

- Cassandra Configuration
- Cassandra Sampler
- Cassandra PreProcessor
- Cassandra PostProcessor


## Installation

Simply drop the JMeterCassandra.jar into the JMeter's lib dirctory, and its dependent libraries in the the lib/ext directory.

## Configuration

Because it is based on the Java Driver, the plugin automatically connects to a given contact point and will discover the rest of the nodes in the cluster.  To function correclty, it is necessary that JMeter be able to directly access all of the nodes in your cluster

Fields:
- Variable Name (Required): Similar to JDBC.  A name by which the Samplers/Processors will refer to this connection
- Contact Points (Required):  A comma-separated list of contact points in your cluster
- Default Keyspace (Optional):  The default keyspace used by CQL
- Username (Future):
- Password (Future):


![alt text](https://github.com/slowenthal/jmeter-cassandra/blob/master/wiki/images/configScreenShot.png)
