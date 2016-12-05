/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.metrics2.reporters;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
  
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;

import org.apache.storm.Config;
import org.apache.storm.cluster.DaemonType;
import org.apache.storm.metrics2.reporters.PreparableReporter;
import org.apache.storm.utils.Utils;

public class MetricReporterConfig {

    private static final Logger LOG = LoggerFactory.getLogger(MetricReporterConfig.class);

    private MetricRegistry _registry;
    private DaemonType _daemonType;
    private String _daemonId;
    private Map<String, PreparableReporter> _reporters;

    public MetricReporterConfig(MetricRegistry registry, DaemonType daemonType, String daemonId){
        _registry = registry;
        _daemonType = daemonType;
        _daemonId = daemonId;
        _reporters = new HashMap<String, PreparableReporter>();
    }

    public void stop() {
        for (Map.Entry<String, PreparableReporter> entry : _reporters.entrySet()) {
            entry.getValue().stop();
        }
    }

    public void configure(Map conf) {
        Map<String, Object> reporterConfigs = (Map<String, Object>)conf.get(Config.STORM_METRICS_REPORTERS);

        for (String reporterName : reporterConfigs.keySet()) {
            Map<String, Object> reporterConfig = (Map<String, Object>)reporterConfigs.get(reporterName);
            String reporterClass = Utils.getString(reporterConfig.get("class"));

            Set<String> daemonTypes = new HashSet<String>((List<String>)reporterConfig.get("daemons"));

            // check if the reporter wants to be configured for this daemon type
            if (daemonTypes.contains(_daemonType.name().toLowerCase())){
                LOG.info("Configuring metrics reporter {} of type {} for daemon type {} and id {}", 
                        reporterName, reporterClass, _daemonType, _daemonId);
                LOG.debug("Reporter config {}", reporterConfig);

                try {
                    PreparableReporter reporter = (PreparableReporter)Utils.newInstance(reporterClass);
                    _reporters.put(reporterName, reporter);
                    reporter.prepare(_registry, conf, reporterConfig, _daemonId);
                    reporter.start();

                    LOG.info("Started statistics report plugin {}", reporterName);

                } catch (RuntimeException e){
                    LOG.error("Unable to configure metrics reporter", e);
                }
            }
        }
    }
}
