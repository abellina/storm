package org.apache.storm.metrics2.store;

/**
 * @see HBaseSerializer
 */
enum HBaseSchemaType {
    COMPACT("compact", "org.apache.storm.metrics2.store.HBaseSerializerCompact"),
    EXPANDED("expanded", "org.apache.storm.metrics2.store.HBaseSerializerExpanded");

    private String configKey;
    private String className;

    HBaseSchemaType(String configKey, String className) {
        this.configKey = configKey;
        this.className = className;
    }

    public String getClassName() {
        return this.className;
    }

    public static HBaseSchemaType fromKey(String key) {
        for (HBaseSchemaType type : HBaseSchemaType.values()) {
            if (type.configKey.equals(key))
                return type;
        }
        throw new IllegalArgumentException("Invalid schema type");
    }

}


enum HBaseMetadataIndex {

    TOPOLOGY(0, "topoMap"),
    STREAM(1, "streamMap"),
    HOST(2, "hostMap"),
    COMP(3, "compMap"),
    METRICNAME(4, "metricMap"),
    EXECUTOR(5, "executorMap");

    private int index;
    private String schemaMapping;

    HBaseMetadataIndex(int index, String schemaMapping) {
        this.index = index;
        this.schemaMapping = schemaMapping;
    }

    public int getIndex() {
        return index;
    }

    public String getSchemaMapping() {
        return schemaMapping;
    }

    public static int indexFromMapping(String mapping) {
        for (HBaseMetadataIndex item : HBaseMetadataIndex.values()) {
            if (item.schemaMapping.equals(mapping))
                return item.index;
        }
        throw new IllegalArgumentException("Invalid schema type");
    }

    public static int count() {
        return HBaseMetadataIndex.values().length;
    }
}