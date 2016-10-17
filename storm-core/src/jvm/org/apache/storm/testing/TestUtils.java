package org.apache.storm.testing;

public class TestUtils {
    private static String getLocalTempPath() {
        String separator = Utils.isOnWindows() ? "" : "/";
        return System.getProperty("java.io.tmpdir") + separator + Utils.uuid();
    } 

    public static String withLocalTemp() {
    }
}
