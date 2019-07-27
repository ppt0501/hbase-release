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
package org.apache.hadoop.hbase.master;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.mob.MobConstants;

public class MobFileCompactionChore extends ScheduledChore {

  private static final Log LOG = LogFactory.getLog(MobFileCompactionChore.class);
  private final Configuration conf;
  private final HMaster master;
  private volatile boolean running = false;
  
  public MobFileCompactionChore(HMaster master) {
    super(master.getServerName() + "-MobFileCompactionChore", master, master.getConfiguration()
      .getInt(MobConstants.MOB_COMPACTION_CHORE_PERIOD, 
        MobConstants.DEFAULT_MOB_COMPACTION_CHORE_PERIOD), master
      .getConfiguration().getInt(MobConstants.MOB_COMPACTION_CHORE_PERIOD,
        MobConstants.DEFAULT_MOB_COMPACTION_CHORE_PERIOD), TimeUnit.SECONDS);
    this.master = master;
    this.conf = master.getConfiguration();
  
  }

  @Override
  protected void chore() {
    try (Connection conn = ConnectionFactory.createConnection(conf);
         Admin admin = conn.getAdmin(); ) {
      
      if (running) {
        LOG.warn(getName() +" is running already, skipping this attempt.");
        return;
      }
      running = true;
      TableDescriptors htds = master.getTableDescriptors();
      Map<String, HTableDescriptor> map = htds.getAll();
      for (HTableDescriptor htd : map.values()) {
        for (HColumnDescriptor hcd : htd.getColumnFamilies()) {
          if (hcd.isMobEnabled()) {
            LOG.info("DDDD Major compacting "+ htd.getTableName() + " cf=" + hcd.getNameAsString());
            admin.majorCompact(htd.getTableName(), hcd.getName());           
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to compact", e);
    } finally {
      running = false;
    }
  }
}
