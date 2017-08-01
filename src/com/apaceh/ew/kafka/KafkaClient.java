package com.apaceh.ew.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.log4j.Logger;

import com.apaceh.ew.kafka.utils.Constant;
import com.apaceh.ew.kafka.utils.ServerConfiguration;

import kafka.admin.AdminUtils;
import kafka.api.PartitionMetadata;
import kafka.api.TopicMetadata;
import kafka.api.TopicMetadataRequest;
import kafka.api.TopicMetadataResponse;
import kafka.consumer.SimpleConsumer;
import kafka.utils.ZkUtils;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;

/**
 * Client for kafka.
 * 
 * @author EthanWang
 *
 */
public class KafkaClient {
	private static final Logger LOG = Logger.getLogger(KafkaClient.class);
	private static KafkaClient instance;
	private List<String> brokers = new ArrayList<>();
	private int port = -1;
	private ZkUtils zookeeper;

	public static KafkaClient getInstance() {
		if (instance == null) {
			synchronized (KafkaClient.class) {
				if (instance == null) {
					instance = new KafkaClient();
				}
			}
		}
		return instance;
	}

	/**
	 * Get partition number of specified topic.
	 * 
	 * @param topic
	 * @return
	 */
	public int getPartitionCount(String topic) {
		TopicMetadata meta = AdminUtils.fetchTopicMetadataFromZk(topic, zookeeper);
		int number = meta.partitionsMetadata().size();
		if (number <= 0) {
			LOG.error("Partition number is negtive: " + topic);
			throw new RuntimeException("Partition number is negtive: " + topic);
		}
		return number;
	}

	/**
	 * Get the retention size of each partition.
	 * 
	 * @return
	 */
	public long getRetensionSize(String topic) {
		Properties map = AdminUtils.fetchEntityConfig(zookeeper, "topics", topic);
		String retention = map.getProperty("retention.bytes");
		if (retention == null) {
			return -1;
		}
		return Long.valueOf(retention);
	}

	/**
	 * Get used size of specified topic in Bytes.
	 * 
	 * @param topic
	 * @return
	 */
	public long fetchTopicSize(String topic) {
		Set<Partition> partitions = getLeaderPartitions(topic);
		long sizeBytes = 0l;
		for (Partition par : partitions) {
			sizeBytes = sizeBytes + partitionSize(par);
		}
		return sizeBytes;
	}

	private long partitionSize(Partition par) {
		try {
			MBeanServerConnection conn = KafkaJMXPool.getConnection(par.getHost());
			Object value = conn.getAttribute(MBeanName.name(par.getTopic(), par.getPar()), MBeanName.VALUE);
			return (Long) value;
		} catch (Exception e) {
			LOG.error("Fetching partition size failed: " + par, e);
			;
			throw new RuntimeException("Fetching partition size failed: " + par, e);
		}
	}

	/**
	 * Get all the leader partitions of specified topic.
	 * 
	 * @param topicName
	 * @return
	 */
	private Set<Partition> getLeaderPartitions(String topicName) {
		// Set<Partition> partitions = new HashSet<>();
		// TopicMetadata meta = AdminUtils.fetchTopicMetadataFromZk(topicName,
		// zookeeper);
		// for(PartitionMetadata partitionMeta :
		// toList(meta.partitionsMetadata())){
		// String allocatedHost = partitionMeta.leader().get().host();
		// partitions.add(new Partition(topicName, partitionMeta.partitionId(),
		// allocatedHost));
		// }
		// return partitions;
		Set<Partition> partitions = new HashSet<>();
		for (String broker : this.brokers) {
			SimpleConsumer consumer = new SimpleConsumer(broker, this.port, 30000, 30000, "777");
			TopicMetadataRequest request = new TopicMetadataRequest(topic(topicName), 999);
			TopicMetadataResponse rsp = consumer.send(request);
			for (TopicMetadata topicMeta : toList(rsp.topicsMetadata())) {
				for (PartitionMetadata partitionMeta : toList(topicMeta.partitionsMetadata())) {
					String allocatedHost = partitionMeta.leader().get().host();
					partitions.add(new Partition(topicName, partitionMeta.partitionId(), allocatedHost));
				}
			}
			consumer.close();
		}
		return partitions;
	}

	/**
	 * Put topic name in seq
	 * 
	 * @param topicName
	 * @return
	 */
	private Seq<String> topic(String topicName) {
		return toSeq(toList(topicName));
	}

	private List<String> toList(String name) {
		return Arrays.asList(new String[] { name });
	}

	private KafkaClient() {
		String[] hosts = ServerConfiguration.getConf().getProperty("ew.kafka.brokers").trim().split(",");
		this.brokers.addAll(Arrays.asList(hosts));
		this.port = Integer.valueOf(ServerConfiguration.getConf().getProperty("ew.kafka.broker.port").trim());
		initZK();
	}

	private void initZK() {
		if (JaasUtils.isZkSecurityEnabled()) {
			String jaas = ServerConfiguration.getConf().getProperty("ew.kafka.security.jaas.file");
			if (jaas == null || jaas.isEmpty()) {
				throw new RuntimeException("Kafka jaas file not configured.");
			}
			System.setProperty("java.security.auth.login.config", jaas);
		}
		LOG.info("Zookeeper isZkSecurityEnabled : " + JaasUtils.isZkSecurityEnabled());
		Tuple2<ZkClient, ZkConnection> tuple = ZkUtils.createZkClientAndConnection(assembleZKStr(), 30000, 30000);
		zookeeper = new ZkUtils(tuple._1, tuple._2, JaasUtils.isZkSecurityEnabled());
	}

	private String assembleZKStr() {
		String[] zk = ServerConfiguration.getConf().getProperty(Constant.ZOOKEEPER).split(",");
		String port = ServerConfiguration.getConf().getProperty(Constant.ZOOKEEPER_PORT);
		StringBuilder sb = new StringBuilder();
		for (String item : zk) {
			sb.append(item).append(":").append(port).append(",");
		}
		String zkStr = sb.toString();
		String finalString = zkStr.substring(0, zkStr.length() - 1);
		LOG.info("Zookeeper connection string for KafkaClient: " + finalString);
		return finalString;
	}

	/**
	 * Kafka partition, consist of parent topic, partitionid and allocated host
	 * of this partition.
	 * 
	 * @author EthanWang
	 *
	 */
	private static class Partition {
		private String topic;
		private int par; // partition number.
		private String host; // host at which partition locates.

		public Partition(String topic, int partition, String host) {
			this.par = partition;
			this.host = host;
			this.topic = topic;
		}

		public int getPar() {
			return par;
		}

		public String getTopic() {
			return topic;
		}

		public String getHost() {
			return host;
		}

		public String toString() {
			return topic + "-" + par + "@" + host;
		}

		public boolean equals(Object obj) {
			return this.toString().equals(obj.toString());
		}

		public int hashCode() {
			return -1;
		}
	}

	/**
	 * JMX connections cache pool for Kafka.
	 * 
	 * @author EthanWang
	 *
	 */
	private static class KafkaJMXPool {
		private static final String URL = "service:jmx:rmi:///jndi/rmi://";
		private static Map<String, JMXConnector> jmx = new ConcurrentHashMap<>();

		public static MBeanServerConnection getConnection(String host) {
			try {
				if (jmx.containsKey(host)) {
					JMXConnector connector = jmx.get(host);
					return connector.getMBeanServerConnection();
				}
				cache(host, create(host));
				return jmx.get(host).getMBeanServerConnection();
			} catch (Exception e) {
				LOG.error("Error while creating KafkaJMX connection: " + host, e);
				throw new RuntimeException("Error while creating KafkaJMX connection: " + host, e);
			}
		}

		private static void cache(String host, JMXConnector conn) {
			jmx.put(host, conn);
		}

		private static JMXConnector create(String host) {
			try {
				JMXServiceURL url = new JMXServiceURL(URL + host + ":" + "9999/jmxrmi");
				JMXConnector jmxc = JMXConnectorFactory.connect(url);
				return jmxc;
			} catch (Exception e) {
				LOG.error("Creating KafkaJMX connection failed: " + host, e);
				throw new RuntimeException("Creating KafkaJMX connection failed: " + host, e);
			}
		}
	}

	/**
	 * MBean name constructor.
	 * 
	 * @author EthanWang
	 *
	 */
	private static class MBeanName {
		private static final String OBJECTSTR = "kafka.log:type=Log,name=Size,topic={%TOPIC},partition={%PARTITION}";
		public static final String VALUE = "Value";

		public static ObjectName name(String topic, int partition) throws MalformedObjectNameException {
			return new ObjectName(
					OBJECTSTR.replace("{%TOPIC}", topic).replace("{%PARTITION}", String.valueOf(partition)));
		}
	}

	private <T> Seq<T> toSeq(List<T> list) {
		return JavaConversions.asScalaBuffer(list).seq();
	}

	private <T> List<T> toList(Seq<T> seq) {
		return JavaConversions.asJavaList(seq);
	}

}
