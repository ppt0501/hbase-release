/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.mob;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.master.cleaner.TimeToLiveHFileCleaner;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestMobStressTool {
  private HBaseTestingUtility HTU = new HBaseTestingUtility();

  @Before
  public void setUp() throws Exception {
    Configuration conf = HTU.getConfiguration();
    initConf(conf);
    HTU.startMiniCluster();
  }

  private void initConf(Configuration conf) {

    conf.setInt("hfile.format.version", 3);
    //conf.setLong(TimeToLiveHFileCleaner.TTL_CONF_KEY, 0);
    conf.setLong("hbase.master.cleaner.interval", 60000);
    conf.setInt("hbase.client.retries.number", 100);
    conf.setInt("hbase.hregion.max.filesize", 200000000);
    conf.setInt("hbase.hregion.memstore.flush.size", 800000);
    conf.setInt("hbase.hstore.blockingStoreFiles", 150);
    conf.setInt("hbase.hstore.compaction.throughput.lower.bound", 52428800);
    conf.setInt("hbase.hstore.compaction.throughput.higher.bound", 2*52428800);
    // Disable MOB cleaner every
    conf.setInt(MobConstants.MOB_CLEANER_PERIOD, 0);
    conf.set(MobStoreEngine.DEFAULT_MOB_COMPACTOR_CLASS_KEY,
      FaultyMobStoreCompactor.class.getName());
    conf.setBoolean(MobConstants.MOB_DISCARD_MISS_KEY, true);
    // Set minimum MOB file age to 60 sec
    conf.setLong(MobConstants.MOB_MINIMUM_FILE_AGE_TO_ARCHIVE_KEY, 60000);
    // Disable periodic MOB compactions
    conf.setInt(MobConstants.MOB_COMPACTION_CHORE_PERIOD, 0);

  }

  @Test
  public void testMobStressTool () throws IOException, Exception {
    String[] args = new String[] {"-n", "10000000"};
    int ret = ToolRunner.run(HTU.getConfiguration(), new MobStressTool(), args);
    assertEquals(0,ret);
    System.exit(ret);
  }

  @After
  public void tearDown() throws Exception {
    HTU.shutdownMiniCluster();
  }

}
