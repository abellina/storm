package org.apache.storm.hbase.metrics2.store;

import org.apache.storm.generated.Window;

import java.util.Arrays;

//TODO: this should be an internal enum

public class TimeRange {
    public Long startTime;
    public Long endTime;
    public Window window;

    public TimeRange(Long startTime, Long endTime, Window window){
        this.startTime = startTime;
        this.endTime = endTime;
        this.window = window;
    }

    public TimeRange() {
        this.startTime = null;
        this.endTime = null;
        this.window = Window.ALL; //?
    }

    public boolean contains(Long time){
        if ((startTime == null || (startTime != null && time >= startTime)) && 
            (endTime == null || (endTime != null && time <= endTime))){
            return true;
        }
        return startTime == null && endTime == null;
    }

    public String toString(){
        return "start: " + this.startTime + " end: " + this.endTime + " window: " + this.window;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(new Object[] {
            startTime,
            endTime,
            window.getValue()
        });
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        TimeRange rhs = (TimeRange) o;
        return rhs.startTime == this.startTime &&
               rhs.endTime == this.endTime &&
               rhs.window == this.window;
    }
}
