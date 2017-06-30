package org.apache.storm.hbase.metrics2.store;

import org.apache.storm.hbase.metrics2.store.Metric;
import org.apache.storm.hbase.metrics2.store.TimeRange;

import java.util.Set;

public interface IAggregator {
    public void agg(Metric metric, Set<TimeRange> timeRanges);
}
