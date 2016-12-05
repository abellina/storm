/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.metrics2.reporters;

import com.codahale.metrics.graphite.GraphiteReporter;
import com.codahale.metrics.graphite.GraphiteSender;
import com.codahale.metrics.graphite.GraphiteUDP;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.MetricRegistry;
import org.apache.storm.daemon.metrics.MetricsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class GraphitePreparableReporter implements PreparableReporter<GraphiteReporter> {
    private final static Logger LOG = LoggerFactory.getLogger(GraphitePreparableReporter.class);
    GraphiteReporter reporter = null;

    long reportingPeriod;
    TimeUnit reportingPeriodUnit;

    @Override
    public void prepare(MetricRegistry metricsRegistry, Map stormConf, Map reporterConf, String daemonId) {
        LOG.debug("Preparing...");
        GraphiteReporter.Builder builder = GraphiteReporter.forRegistry(metricsRegistry);

        TimeUnit durationUnit = MetricsUtils.getMetricsDurationUnit(reporterConf);
        if (durationUnit != null) {
            builder.convertDurationsTo(durationUnit);
        }

        TimeUnit rateUnit = MetricsUtils.getMetricsRateUnit(reporterConf);
        if (rateUnit != null) {
            builder.convertRatesTo(rateUnit);
        }

        //TODO: expose some simple MetricFilters 
        String prefix = MetricsUtils.getMetricsPrefixedWith(reporterConf);
        if (prefix != null) {
            builder.prefixedWith(prefix);
        }

        //defaults to 10
        reportingPeriod = MetricsUtils.getMetricsSchedulePeriod(reporterConf);

        //defaults to seconds
        reportingPeriodUnit = MetricsUtils.getMetricsSchedulePeriodUnit(reporterConf);

        // Not exposed:
        // * withClock(Clock)

        String host = MetricsUtils.getMetricsTargetHost(reporterConf);
        Integer port = MetricsUtils.getMetricsTargetPort(reporterConf);
        String transport = MetricsUtils.getMetricsTargetTransport(reporterConf);
        GraphiteSender sender = null;
        //TODO: error checking
        if (transport.equalsIgnoreCase("udp")) {
            sender = new GraphiteUDP(host, port);
        } else {
            //TODO: pickled support
            sender = new Graphite(host, port);
        }
        reporter = builder.build(sender);
    }

    @Override
    public void start() {
        if (reporter != null) {
            LOG.debug("Starting...");
            reporter.start(reportingPeriod, reportingPeriodUnit);
        } else {
            throw new IllegalStateException("Attempt to start without preparing " + getClass().getSimpleName());
        }
    }

    @Override
    public void stop() {
        if (reporter != null) {
            LOG.debug("Stopping...");
            reporter.stop();
        } else {
            throw new IllegalStateException("Attempt to stop without preparing " + getClass().getSimpleName());
        }
    }
}
