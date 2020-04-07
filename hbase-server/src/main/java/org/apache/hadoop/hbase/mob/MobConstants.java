/**
 *
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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * The constants used in mob.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MobConstants {

  public static final String MOB_SCAN_RAW = "hbase.mob.scan.raw";
  public static final String MOB_CACHE_BLOCKS = "hbase.mob.cache.blocks";
  public static final String MOB_SCAN_REF_ONLY = "hbase.mob.scan.ref.only";
  public static final String EMPTY_VALUE_ON_MOBCELL_MISS = "empty.value.on.mobcell.miss";

  public static final String MOB_FILE_CACHE_SIZE_KEY = "hbase.mob.file.cache.size";
  public static final int DEFAULT_MOB_FILE_CACHE_SIZE = 1000;

  public static final String MOB_DIR_NAME = "mobdir";
  public static final String MOB_REGION_NAME = ".mob";
  public static final byte[] MOB_REGION_NAME_BYTES = Bytes.toBytes(MOB_REGION_NAME);

  public static final String MOB_CLEANER_PERIOD = "hbase.master.mob.cleaner.period";
  public static final int DEFAULT_MOB_CLEANER_PERIOD = 24 * 60 * 60; // one day

  public static final String MOB_SWEEP_TOOL_COMPACTION_START_DATE =
      "hbase.mob.sweep.tool.compaction.start.date";
  public static final String MOB_SWEEP_TOOL_COMPACTION_RATIO =
      "hbase.mob.sweep.tool.compaction.ratio";
  public static final String MOB_SWEEP_TOOL_COMPACTION_MERGEABLE_SIZE =
      "hbase.mob.sweep.tool.compaction.mergeable.size";

  public static final float DEFAULT_SWEEP_TOOL_MOB_COMPACTION_RATIO = 0.5f;
  public static final long DEFAULT_SWEEP_TOOL_MOB_COMPACTION_MERGEABLE_SIZE = 128 * 1024 * 1024;

  public static final String MOB_SWEEP_TOOL_COMPACTION_TEMP_DIR_NAME = "mobcompaction";

  public static final String MOB_SWEEP_TOOL_COMPACTION_MEMSTORE_FLUSH_SIZE =
      "hbase.mob.sweep.tool.compaction.memstore.flush.size";
  public static final long DEFAULT_MOB_SWEEP_TOOL_COMPACTION_MEMSTORE_FLUSH_SIZE =
      1024 * 1024 * 128; // 128M

  public static final String MOB_CACHE_EVICT_PERIOD = "hbase.mob.cache.evict.period";
  public static final String MOB_CACHE_EVICT_REMAIN_RATIO = "hbase.mob.cache.evict.remain.ratio";
  public static final Tag MOB_REF_TAG = new Tag(TagType.MOB_REFERENCE_TAG_TYPE,
      HConstants.EMPTY_BYTE_ARRAY);

  public static final float DEFAULT_EVICT_REMAIN_RATIO = 0.5f;
  public static final long DEFAULT_MOB_CACHE_EVICT_PERIOD = 3600l;

  public final static String TEMP_DIR_NAME = ".tmp";
  public final static String BULKLOAD_DIR_NAME = ".bulkload";
  public final static byte[] MOB_TABLE_LOCK_SUFFIX = Bytes.toBytes(".mobLock");
  public final static String EMPTY_STRING = "";
  /**
   * If the size of a mob file is less than this value, it's regarded as a small file and needs to
   * be merged in mob compaction. The default value is 1280MB.
   */
  public static final String MOB_COMPACTION_MERGEABLE_THRESHOLD =
    "hbase.mob.compaction.mergeable.threshold";
  public static final long DEFAULT_MOB_COMPACTION_MERGEABLE_THRESHOLD = 10 * 128 * 1024 * 1024;
  /**
   * The max number of del files that is allowed in the mob file compaction. In the mob
   * compaction, when the number of existing del files is larger than this value, they are merged
   * until number of del files is not larger this value. The default value is 3.
   */
  public static final String MOB_DELFILE_MAX_COUNT = "hbase.mob.delfile.max.count";
  public static final int DEFAULT_MOB_DELFILE_MAX_COUNT = 3;
  /**
   * The max number of the mob files that is allowed in a batch of the mob compaction.
   * The mob compaction merges the small mob files to bigger ones. If the number of the
   * small files is very large, it could lead to a "too many opened file handlers" in the merge.
   * And the merge has to be split into batches. This value limits the number of mob files
   * that are selected in a batch of the mob compaction. The default value is 100.
   */
  public static final String MOB_COMPACTION_BATCH_SIZE =
    "hbase.mob.compaction.batch.size";
  public static final int DEFAULT_MOB_COMPACTION_BATCH_SIZE = 100;
  /**
   * The period that MobCompactionChore runs. The unit is second.
   * The default value is one week.
   */
  public static final String MOB_COMPACTION_CHORE_PERIOD =
    "hbase.mob.compaction.chore.period";
  public static final int DEFAULT_MOB_COMPACTION_CHORE_PERIOD =
    24 * 60 * 60 * 7; // a week
  public static final String MOB_COMPACTOR_CLASS_KEY = "hbase.mob.compactor.class";
  /**
   * The max number of threads used in MobCompactor.
   */
  public static final String MOB_COMPACTION_THREADS_MAX =
    "hbase.mob.compaction.threads.max";
  public static final int DEFAULT_MOB_COMPACTION_THREADS_MAX = 1;

  /**
   * Is MOB compaction automatic (true - then MOB compaction runs with every major compaction,
   * false - MOB compaction is user - originated only)
   */
  public static final String MOB_COMPACTION_AUTO_ENABLED_KEY = "hbase.mob.compactions.auto";
  public static final boolean DEFAULT_MOB_COMPACTION_AUTO_ENABLED = true;

  /**
   * How aggressively will MOB auto compaction work (0 - never, 1.0 - always)
   * This is the probability of a running MOB compaction during system - originated
   * regular major compaction (not user - originated), ranges from 0. (never) to 1. (always)
   *
   */

  public static final String MOB_COMPACTION_AUTO_PROB_KEY = "hbase.mob.compactions.prob";
  public static final double DEFAULT_MOB_COMPACTION_AUTO_PROB = 1.0;

  /**
   * Maximum size of a MOB compaction selection
   */
  public static final String MOB_COMPACTION_MAX_SELECTION_SIZE_KEY =
      "hbase.mob.compactions.max.selection.size";
  /**
   * Default maximum selection size = 500MB
   */
  public static final long DEFAULT_MOB_COMPACTION_MAX_SELECTION_SIZE = 500 * 1024 * 1024;


  /**
   * Minimum number of MOB files eligible for compaction
   */
  public static final String MOB_COMPACTION_MIN_FILES_KEY = "hbase.mob.compactions.min.files";

  public static final int DEFAULT_MOB_COMPACTION_MIN_FILES = 3;

  public static final String MOB_DISCARD_MISS_KEY = "hbase.mob.discard.miss";

  public static final boolean DEFAULT_MOB_DISCARD_MISS = false;

  public static final String MOB_MINIMUM_FILE_AGE_TO_ARCHIVE_KEY = "mob.minimum.file.age.to.archive";

  public static final long DEFAULT_MOB_MINIMUM_FILE_AGE_TO_ARCHIVE = 3600000; // 1 hour

  private MobConstants() {

  }
}
