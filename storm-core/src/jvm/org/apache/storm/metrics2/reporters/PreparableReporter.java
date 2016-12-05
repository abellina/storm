package org.apache.storm.metrics2.reporters;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reporter;

import java.io.Closeable;
import java.util.Map;

import org.apache.storm.cluster.DaemonType;

public interface PreparableReporter<T extends Reporter & Closeable> {
    void prepare(MetricRegistry metricsRegistry, Map conf, Map reporterConf, String daemonId);
    void start();
    void stop();
}
