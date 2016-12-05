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
package org.apache.storm.metric;

import org.apache.storm.Config;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.apache.storm.generated.LSWorkerStats;
import org.apache.storm.generated.SupervisorWorkerStats;

import java.util.List;
import java.util.Map;

public class StatsPusher {

    NimbusClient client = null;
    private int bufferSize = 4096;
    private String _supervisorId;

    public StatsPusher(String supervisorId) {
        _supervisorId = supervisorId;
    }

    public void prepare(Map conf) {
        this.client = NimbusClient.getConfiguredClient(conf);
        if (conf != null) {
            this.bufferSize = Utils.getInt(conf.get(Config.STORM_BLOBSTORE_INPUTSTREAM_BUFFER_SIZE_BYTES), bufferSize);
        }
    }

    public void sendWorkerStatsToNimbus(SupervisorWorkerStats sws) {
        try {
            this.client.getClient().consumeWorkerStats(sws);
        } catch (org.apache.thrift.TException ex) {
            System.out.println("ayayay!!" + ex);
        }
    }
}
