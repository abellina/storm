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
package org.apache.storm.hbase.metrics2.store;

import java.io.IOException;
import java.lang.String;

import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.util.*;

import org.apache.commons.codec.binary.Hex;

import java.io.File;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseSerializer {
    private final static Logger LOG = LoggerFactory.getLogger(HBaseSerializer.class);


    public class SerializationResult {
        public byte[] SerialKey;
        public byte[] SerialMetaKey;
    }

    public byte[] serializeKey(Metric m) {
        ByteBuffer buf = ByteBuffer.allocate(34);
        buf.put((byte) 0);
        buf.put(m.getAggLevel());
        buf.putInt(0);
        buf.putLong(m.getTimeStamp());
        buf.putInt(0);
        buf.putInt(0);
        buf.putInt(0);
        buf.putInt(0);
        buf.putLong(m.getPort());
        buf.putInt(0);

        return buf.array();
    }


}
