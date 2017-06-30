package org.apache.storm.metrics2.store;

class ConfigKeywords {

    final static String BASE_CONFIG_KEY = "storm.metrics2.store.HBaseStore";

    final static String HBASE_ZK_KEY = BASE_CONFIG_KEY + ".zookeeper";
    final static String STORM_ZK_KEY = "storm.zookeeper";

    final static String ZOOKEEPER_SERVERS = ".servers";
    final static String ZOOKEEPER_PORT = ".port";
    final static String ZOOKEEPER_ROOT = ".root";
    final static String ZOOKEEPER_SESSION_TIMEOUT = ".session.timeout";

    final static String HBASE_ROOT_DIR = BASE_CONFIG_KEY + ".hbase.root_dir";

    final static String SCHEMA_KEY = BASE_CONFIG_KEY + ".hbase.schema";

}
