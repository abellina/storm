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
import org.apache.storm.generated.Window;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class HBaseStoreTest {

    private static final String SCHEMA_TYPE = "compact";

    private static HBaseStore store;
    private static HBaseTestingUtility testUtil;
    private static HTableInterface metricsTable;
    private static Random random = new Random();

    private static HashMap<String, Object> makeConfig() {

        HashMap<String, Object> confMap = new HashMap<>();
        // metadata map
        HashMap<String, Object> metaDataMap = new HashMap<>();
        List<String> metadataNames = Arrays.asList("topoMap", "streamMap", "hostMap",
                "compMap", "metricMap", "executorMap");

        metadataNames.forEach((name) -> {
            HashMap<String, String> m = new HashMap<>();
            m.put("name", "metrics");
            m.put("cf", "m");
            m.put("column", "c");
            m.put("refcounter", "REFCOUNTER");
            metaDataMap.put(name, m);
        });

        // columns map & metrics map
        HashMap<String, String> columnsMap = new HashMap<>();
        columnsMap.put("value", "v");
        columnsMap.put("sum", "s");
        columnsMap.put("count", "c");
        columnsMap.put("min", "i");
        columnsMap.put("max", "a");

        HashMap<String, Object> metricsMap = new HashMap<>();
        metricsMap.put("name", "metrics");
        metricsMap.put("cf", "c");
        if (SCHEMA_TYPE.equals("compact"))
            metricsMap.put("column", "c");
        else if (SCHEMA_TYPE.equals("expanded"))
            metricsMap.put("columns", columnsMap);

        // schema map
        HashMap<String, Object> schemaMap = new HashMap<>();
        schemaMap.put("type", SCHEMA_TYPE);
        schemaMap.put("metrics", metricsMap);
        schemaMap.put("metadata", metaDataMap);

        confMap.put("storm.metrics2.store.HBaseStore.hbase.schema", schemaMap);


        return confMap;
    }

    private static Metric makeMetric(long ts) {

        Metric m = new Metric("testMetric", ts,
                "testExecutor",
                "testComp",
                "testStream" + String.valueOf(ts),
                "testTopo",
                123.45);
        m.setHost("testHost");
        m.setValue(123.45);
        return m;
    }

    private static Metric makeAggMetric(long ts) {

        Metric m = makeMetric(ts);
        m.setAggLevel((byte) 1);
        m.setValue(100.0);

        for (int i = 1; i < 10; ++i) {
            m.updateAverage(100.00 * i);
        }

        return m;
    }


    @BeforeClass
    public static void setUp() {

        HashMap<String, Object> conf = makeConfig();
        store = new HBaseStore();
        Configuration hbaseConf = HBaseConfiguration.create();

        testUtil = new HBaseTestingUtility(hbaseConf);

        try {
            testUtil.startMiniCluster();
            initSchema(conf, testUtil.getHBaseAdmin());
            // set ZK info from test cluster - not the same as passed in above
            int zkPort = testUtil.getZkCluster().getClientPort();
            conf.put("HBaseZookeeperPortOverride", zkPort);

            store.prepare(conf);
            metricsTable = store.getMetricsTable();
        } catch (Exception e) {
            fail("Unexpected exception" + e);
        }
    }

    private static void initSchema(Map conf, HBaseAdmin admin) {
        try {
            HBaseSchema                                      schema   = new HBaseSchema(conf);
            HashMap<TableName, ArrayList<HColumnDescriptor>> tableMap = schema.getTableMap();

            for (Map.Entry<TableName, ArrayList<HColumnDescriptor>> entry : tableMap.entrySet()) {

                TableName               name        = entry.getKey();
                List<HColumnDescriptor> columnsList = entry.getValue();

                HTableDescriptor descriptor = new HTableDescriptor(name);
                for (HColumnDescriptor columnDescriptor : columnsList) {
                    descriptor.addFamily(columnDescriptor);
                }
                admin.createTable(descriptor);

            }
        } catch (Exception e) {
            fail("Unexpected exception - " + e);
        }
    }

    @After
    public void sleep() {
        // add an artificial delay to avoid comodification errors
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            fail("Unexpected exception - " + e);
        }
    }

    @AfterClass
    public static void tearDown() {
        try {
            testUtil.shutdownMiniCluster();
        } catch (Exception e) {
            fail("Unexpected - " + e);
        }
    }

    @Test
    public void testInsert() {
        Metric m = makeMetric(1234567);
        store.insert(m);

        ResultScanner scanner;
        Scan          s = new Scan();
        try {
            scanner = metricsTable.getScanner(s);
        } catch (Exception e) {
            fail("Unexpected - " + e);
            return;
        }

        for (Result result : scanner) {
            long ts = result.rawCells()[0].getTimestamp();
            if (m.getTimeStamp() == ts) {
                return;
            }
        }

        fail("Could not find metric in store.");

    }

    @Test
    public void testInsertAgg() {
        Metric m = makeAggMetric(1234567);
        store.insert(m);

        ResultScanner scanner;
        Scan          s = new Scan();
        try {
            scanner = metricsTable.getScanner(s);
        } catch (Exception e) {
            fail("Unexpected - " + e);
            return;
        }

        for (Result result : scanner) {
            long ts = result.rawCells()[0].getTimestamp();
            if (m.getTimeStamp() == ts) {
                return;
            }
        }

        fail("Could not find metric in store.");
    }

    @Test
    public void testScan() {

        ArrayList<Metric> metricsList      = new ArrayList<>(10);
        ArrayList<Metric> retreivedMetrics = new ArrayList<>(10);

        for (int i = 1; i <= 10; i++) {
            Metric m = makeMetric(random.nextInt(9999));
            metricsList.add(m);
            store.insert(m);
        }

        store.scan((metric, timeRanges) -> retreivedMetrics.add(metric));

        assertTrue(retreivedMetrics.containsAll(metricsList));

    }

    @Test
    public void testFilteredScan() {


        ArrayList<Metric> metricsList      = new ArrayList<>(100);
        ArrayList<Metric> retreivedMetrics = new ArrayList<>(100);

        for (int i = 1; i <= 100; i++) {
            Metric m = makeMetric(random.nextInt(9999));
            metricsList.add(m);
            store.insert(m);
        }

        Integer aggLevel  = metricsList.get(0).getAggLevel().intValue();
        String  topoIdStr = metricsList.get(0).getTopoIdStr();

        HashSet<TimeRange> timeRangeSet = new HashSet<>();
        timeRangeSet.add(new TimeRange((long) 0, (long) 5000, Window.ALL));
        timeRangeSet.add(new TimeRange((long) 5000, (long) 10000, Window.ALL));

        HashSet<String> metricsSet = new HashSet<>();
        metricsSet.add(metricsList.get(0).getMetricName());

        String compId     = metricsList.get(0).getCompName();
        String executorId = metricsList.get(0).getExecutor();
        String hostId     = metricsList.get(0).getHost();
        String port       = String.valueOf(metricsList.get(0).getPort());

        List<String> filterOrder = Arrays.asList(StringKeywords.aggLevel,
                StringKeywords.topoId,
                StringKeywords.timeRangeSet,
                StringKeywords.metricSet,
                StringKeywords.component,
                StringKeywords.executor,
                StringKeywords.host,
                StringKeywords.port);

        List<Object> filters = Arrays.asList(aggLevel, topoIdStr, timeRangeSet, metricsSet,
                compId, executorId, hostId, port);


        HashMap<String, Object> settings = new HashMap<>();

        for (int i = 0; i < filterOrder.size(); ++i) {
            retreivedMetrics.clear();
            settings.put(filterOrder.get(i), filters.get(i));
            store.scan(settings, (metric, timeRanges) -> retreivedMetrics.add(metric));
            assertTrue(retreivedMetrics.containsAll(metricsList));
        }

    }


    @Test
    public void testPopulateValue() {

        Metric m = makeMetric(23894);
        store.insert(m);

        Metric newMetric = makeMetric(23894);
        newMetric.setValue(0.00);
        newMetric.setCount(0L);

        // check that newMetric has values populated from inserted metric
        store.populateValue(newMetric);
        assertNotEquals(0L, newMetric.getCount());
        assertNotEquals(0.00, newMetric.getValue(), 0.00001);
        assertNotEquals(0.00, newMetric.getSum(), 0.00001);
        assertNotEquals(0.00, newMetric.getMin(), 0.00001);
        assertNotEquals(0.00, newMetric.getMax(), 0.00001);

        // check that invalid new metric isn't populated
        newMetric.setTopoIdStr("BAD TOPOLOGY");
        newMetric.setValue(0.00);
        newMetric.setCount(0L);
        store.populateValue(newMetric);
        assertEquals(0L, newMetric.getCount());
        assertEquals(0.00, newMetric.getValue(), 0.00001);
        assertEquals(0.00, newMetric.getSum(), 0.00001);
        assertEquals(0.00, newMetric.getMin(), 0.00001);
        assertEquals(0.00, newMetric.getMax(), 0.00001);

    }

    @Test
    public void testRemove() {

        for (int i = 1; i <= 10; ++i) {
            Metric m = makeMetric(i);
            store.insert(m);
        }
        for (int i = 101; i <= 110; ++i) {
            Metric m = makeMetric(i);
            store.insert(m);
        }

        HashMap<String, Object> settings     = new HashMap<>();
        HashSet<TimeRange>      timeRangeSet = new HashSet<>();
        timeRangeSet.add(new TimeRange(1L, 10L + 1L, Window.ALL));
        timeRangeSet.add(new TimeRange(101L, 110L + 1L, Window.ALL));

        settings.put(StringKeywords.aggLevel, 0);
        settings.put(StringKeywords.topoId, "testTopo");
        settings.put(StringKeywords.timeRangeSet, timeRangeSet);

        // scan for inserted metrics, should have all 20
        HashSet<Metric> retrievedMetrics = new HashSet<>();

        store.scan(settings, (metric, timeRanges) -> retrievedMetrics.add(metric));

        assertEquals(20, retrievedMetrics.size());

        retrievedMetrics.clear();

        // remove metrics
        store.remove(settings);

        // scan again, should have nil
        store.scan(settings, (metric, timeRanges) -> retrievedMetrics.add(metric));

        assertEquals(0, retrievedMetrics.size());

    }

}