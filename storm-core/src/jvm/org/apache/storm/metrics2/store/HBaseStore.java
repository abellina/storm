/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.metrics2.store;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class HBaseStore implements MetricStore {
    private final static Logger LOG = LoggerFactory.getLogger(HBaseStore.class);

    private HBaseSerializer _serializer;
    private HTableInterface _metricsTable;

    /**
     * Create HBase instance
     * using the configurations provided via the config map
     *
     * @param config Storm config map
     * @throws MetricException On config validation failure or connection error
     */
    @Override
    public void prepare(Map config) throws MetricException {

        Configuration hbaseConf = HBaseConfiguration.create();

        // testing only
        Integer testUtilPort = (Integer) config.get("HBaseZookeeperPortOverride");
        if (testUtilPort != null) {
            hbaseConf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, testUtilPort);
        }
        // end

        try {
            HConnection hbaseConnection = HConnectionManager.createConnection(hbaseConf);
            HBaseAdmin  hbaseAdmin      = new HBaseAdmin(hbaseConnection);
            HBaseSchema schema          = new HBaseSchema(config);

            validateTables(hbaseAdmin, schema);

            TableName metricsTable = schema.metricsTableInfo.getTableName();
            this._metricsTable = hbaseConnection.getTable(metricsTable);
            this._serializer = HBaseSerializer.createSerializer(hbaseConnection, schema);

        } catch (IOException e) {
            throw new MetricException("Could not connect to hbase " + e);
        }
    }

    /**
     * Validate existing HBase tables to ensure they match conf schema
     * @param admin HBase admin object
     * @param schema HBaseSchema from constructor
     * @throws IOException On admin IO failure
     * @throws MetricException On schema mismatch
     */
    private void validateTables(HBaseAdmin admin, HBaseSchema schema) throws IOException, MetricException {

        HashMap<TableName, ArrayList<HColumnDescriptor>> tableMap = schema.getTableMap();

        for (Map.Entry<TableName, ArrayList<HColumnDescriptor>> entry : tableMap.entrySet()) {

            TableName                    name        = entry.getKey();
            ArrayList<HColumnDescriptor> columnsList = entry.getValue();

            if (!admin.tableExists(name))
                throw new MetricException("Table " + name.getNameAsString() + " does not exist in store");

            HTableDescriptor        currentDescriptor     = admin.getTableDescriptor(name);
            List<HColumnDescriptor> currentColumnFamilies = Arrays.asList(currentDescriptor.getColumnFamilies());

            if (!currentColumnFamilies.containsAll(columnsList))
                throw new MetricException("Table " + name.getNameAsString() + " does not contain all columns");
        }
    }

    /**
     * Inserts metric in store
     *
     * @param m Metric to store
     */
    @Override
    public void insert(Metric m) {
        try {
            Put p = _serializer.createPutOperation(m);
            _metricsTable.put(p);
        } catch (MetricException | IOException e) {
            LOG.error("Could not insert metric ", e);
        }
    }

    /**
     * Scans all metrics in the store
     *
     * @param agg - Metric, TimeRange pair
     * @see IAggregator
     */
    @Override
    public void scan(IAggregator agg) {
        HashMap<String, Object> settings = new HashMap<>();
        scan(settings, agg);
    }

    /**
     * Scan metric store with filters
     *
     * @param settings map of settings to search by
     * @param agg      Metric, TimeRange pair
     * @see IAggregator
     */
    @Override
    public void scan(HashMap<String, Object> settings, IAggregator agg) {

        List<Scan> scanList = _serializer.createScanOperation(settings);

        scanList.forEach(s -> {

            int           numRecordsScanned = 0;
            ResultScanner scanner;
            try {
                scanner = _metricsTable.getScanner(s);
            } catch (IOException e) {
                LOG.error("Could not scan metrics table ", e);
                return;
            }

            for (Result result : scanner) {
                ++numRecordsScanned;
                Metric         metric     = _serializer.deserializeMetric(result);
                Set<TimeRange> timeRanges = getTimeRanges(metric, settings);

                agg.agg(metric, timeRanges);
            }
            LOG.info("Scanned {} records", numRecordsScanned);
            scanner.close();
        });
    }

    /**
     * Find time ranges from metric that match specified settings
     *
     * @param m        metric to read time ranges from
     * @param settings map that contains time range set
     * @return set which contains matched time ranges
     */
    private Set<TimeRange> getTimeRanges(Metric m, HashMap<String, Object> settings) {

        Set<TimeRange> timeRangeSet = (Set<TimeRange>) settings.get(StringKeywords.timeRangeSet);

        if (timeRangeSet == null) {
            return null;
        }

        Set<TimeRange> matchedTimeRanges = new HashSet<>();

        for (TimeRange timeRange : timeRangeSet) {
            Long metricTimeStamp = m.getTimeStamp();
            if (timeRange.contains(metricTimeStamp))
                matchedTimeRanges.add(timeRange);
        }

        return matchedTimeRanges;
    }

    /**
     * Populate metric from store that matches given key
     *
     * @param metric metric with key specified
     * @return whether metric was found or not
     */
    @Override
    public boolean populateValue(Metric metric) {

        byte[] key;

        try {
            key = _serializer.createKey(metric);
        } catch (MetricException e) {
            LOG.error("Bad metric passed to populateValue " + e);
            return false;
        }

        try {
            Get    g      = new Get(key);
            Result result = _metricsTable.get(g);
            return _serializer.populateMetricValue(metric, result);
        } catch (IOException e) {
            LOG.error("Could not read from database ", e);
            return false;
        }

    }

    /**
     * Deletes metrics from store
     *
     * @param settings map of settings to filter by
     */
    @Override
    public void remove(HashMap<String, Object> settings) {

        ArrayList<Row> metricsToRemove = new ArrayList<>();

        scan(settings, (metric, timeRanges) -> {
            try {
                byte[] key = _serializer.createKey(metric);
                Delete d   = new Delete(key);
                metricsToRemove.add(d);
            } catch (MetricException e) {
                LOG.error("Could not create key ", e);
            }
        });

        Result[] results = new Result[metricsToRemove.size()];

        try {
            _metricsTable.batch(metricsToRemove, results);
        } catch (IOException | InterruptedException e) {
            LOG.error("Could not delete metrics " + e);
        }

    }

    // testing only
    HBaseSerializer getSerializer() {
        return this._serializer;
    }

    HTableInterface getMetricsTable() {
        return this._metricsTable;
    }

}
