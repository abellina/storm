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
package org.apache.storm.container.cgroup;

import org.apache.storm.container.cgroup.core.CpuacctCore;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class CgroupCpuStats {

    private CpuacctCore core;
    private Long cpuHz;
    private long previousUser;
    private long previousSystem;

    CgroupCpuStats(CgroupCommon workerGroup) throws IOException {
        this.core = (CpuacctCore) workerGroup.getCores().get(SubSystemType.cpuacct);

        ProcessBuilder pb = new ProcessBuilder("getconf", "CLK_TCK");
        Process p = pb.start();
        BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
        String line = in.readLine().trim();
        this.cpuHz = Long.valueOf(line);
    }

    public Map<String, Long> getStats() throws IOException {

        Map<CpuacctCore.StatType, Long> cpuStat = core.getCpuStat();

        long systemHz = cpuStat.get(CpuacctCore.StatType.system);
        long userHz = cpuStat.get(CpuacctCore.StatType.user);
        long user = userHz - this.previousUser;
        long sys = systemHz - this.previousSystem;
        this.previousUser = userHz;
        this.previousSystem = systemHz;

        HashMap<String, Long> ret = new HashMap<>();
        ret.put("user-ms", (user * 1000L) / cpuHz);
        ret.put("sys-ms", (sys * 1000L) / cpuHz);
        return ret;

    }

}
