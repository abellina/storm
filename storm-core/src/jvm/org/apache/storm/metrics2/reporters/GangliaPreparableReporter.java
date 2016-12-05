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

import com.codahale.metrics.ganglia.GangliaReporter;
import com.codahale.metrics.MetricRegistry;
import info.ganglia.gmetric4j.gmetric.GMetric;
import org.apache.storm.daemon.metrics.MetricsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class GangliaPreparableReporter implements PreparableReporter<GangliaReporter> {
    private final static Logger LOG = LoggerFactory.getLogger(GangliaPreparableReporter.class);
    GangliaReporter reporter = null;

    long reportingPeriod;
    TimeUnit reportingPeriodUnit;

    @Override
    public void prepare(MetricRegistry metricsRegistry, Map stormConf, Map reporterConf, String daemonId) {
        LOG.debug("Preparing...");
        GangliaReporter.Builder builder = GangliaReporter.forRegistry(metricsRegistry);

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

        Integer dmax = MetricsUtils.getGangliaDMax(reporterConf);
        if (prefix != null) {
            builder.withDMax(dmax);
        }

        Integer tmax = MetricsUtils.getGangliaTMax(reporterConf);
        if (prefix != null) {
            builder.withTMax(tmax);
        }

        //defaults to 10
        reportingPeriod = MetricsUtils.getMetricsSchedulePeriod(reporterConf);

        //defaults to seconds
        reportingPeriodUnit = MetricsUtils.getMetricsSchedulePeriodUnit(reporterConf);

        // Not exposed:
        // * withClock(Clock)

        String group = MetricsUtils.getMetricsTargetUDPGroup(reporterConf);
        Integer port = MetricsUtils.getMetricsTargetPort(reporterConf);
        String udpAddressingMode = MetricsUtils.getMetricsTargetUDPAddressingMode(reporterConf);
        Integer ttl = MetricsUtils.getMetricsTargetTtl(reporterConf);

        GMetric.UDPAddressingMode mode = udpAddressingMode.equalsIgnoreCase("multicast") ? 
            GMetric.UDPAddressingMode.MULTICAST : GMetric.UDPAddressingMode.UNICAST;

        try {
            GMetric sender = new GMetric(group, port, mode, ttl);
            reporter = builder.build(sender);
        }catch (IOException ioe){
            //TODO
            LOG.error("Exception in GangliaReporter config", ioe);
        }
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
