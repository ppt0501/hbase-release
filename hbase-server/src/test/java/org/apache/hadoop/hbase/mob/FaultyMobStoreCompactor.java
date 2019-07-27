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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.io.hfile.CorruptHFileException;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile.Writer;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionThroughputController;
import org.apache.hadoop.hbase.util.Bytes;

public class FaultyMobStoreCompactor extends DefaultMobStoreCompactor 
{

  public static AtomicLong mobCounter = new AtomicLong();
  public static AtomicLong totalFailures = new AtomicLong();
  public static AtomicLong totalCompactions = new AtomicLong();
  public static AtomicLong totalMajorCompactions = new AtomicLong();
  
  static double failureProb = 0.1d;
  static Random rnd = new Random();
  

  public FaultyMobStoreCompactor(Configuration conf, Store store) {
    super(conf, store);
    failureProb = conf.getDouble("injected.fault.probability", 0.1);
  }
  
  @Override
  protected boolean performCompaction(FileDetails fd, InternalScanner scanner, CellSink writer,
      long smallestReadPoint, boolean cleanSeqId,
      CompactionThroughputController throughputController, boolean major) throws IOException {
    
    // Clear old mob references
    mobRefSet.get().clear();
    
    totalCompactions.incrementAndGet();
    if (major) {
      totalMajorCompactions.incrementAndGet();
    }
    boolean isUserRequest = userRequest.get();

    boolean compactMOBs = major && isUserRequest;
    boolean discardMobMiss = conf.getBoolean(MobConstants.MOB_DISCARD_MISS_KEY,
                                             MobConstants.DEFAULT_MOB_DISCARD_MISS); 
    
    boolean mustFail = false;
    if (compactMOBs) {
      mobCounter.incrementAndGet();
      double dv = rnd.nextDouble();
      if (dv < failureProb) {
        mustFail = true;
        totalFailures.incrementAndGet();
      }
    }
   
    int bytesWritten = 0;
    
    FileSystem fs = FileSystem.get(conf);
    // Since scanner.next() can return 'false' but still be delivering data,
    // we have to use a do/while loop.
    List<Cell> cells = new ArrayList<Cell>();
    // Limit to "hbase.hstore.compaction.kv.max" (default 10) to avoid OOME
    int closeCheckInterval = HStore.getCloseCheckInterval();
    boolean hasMore;
    Path path = MobUtils.getMobFamilyPath(conf, store.getTableName(), store.getColumnFamilyName());
    byte[] fileName = null;
    Writer mobFileWriter = null;
    long mobCells = 0;
    Tag tableNameTag = new Tag(TagType.MOB_TABLE_NAME_TAG_TYPE, store.getTableName().getName());
    long cellsCountCompactedToMob = 0;
    long cellsCountCompactedFromMob = 0;
    long cellsSizeCompactedToMob = 0;
    long cellsSizeCompactedFromMob = 0;    
    Throwable failure = null;
    Cell mobCell = null;

    long counter = 0;
    long countFailAt = -1;    
    if (mustFail) {
      countFailAt = rnd.nextInt(100); // randomly fail fast
    }
    try {
      try {
        mobFileWriter = mobStore.createWriterInTmp(new Date(fd.latestPutTs), fd.maxKeyCount,
          store.getFamily().getCompression(), store.getRegionInfo().getStartKey());
        fileName = Bytes.toBytes(mobFileWriter.getPath().getName());
      } catch (IOException e) {
        // Bailing out
        LOG.error("Failed to create mob writer, ", e);
        throw e;
      }
      
      if (compactMOBs) {
        // Add the only reference we get for compact MOB case
        // because new store file will have only one MOB reference
        // in this case - of newly compacted MOB file
        mobRefSet.get().add(mobFileWriter.getPath().getName());
      }
      
      ScannerContext scannerContext =
          ScannerContext.newBuilder().setBatchLimit(compactionKVMax).build();
      do {
        hasMore = scanner.next(cells, scannerContext);
        // output to writer:
        for (Cell c : cells) {
          counter++;
          if (compactMOBs) {
            if (MobUtils.isMobReferenceCell(c)) {
                           
              if (counter == countFailAt) {
                LOG.warn("\n\n INJECTED FAULT mobCounter="+mobCounter.get()+"\n\n");
                throw new CorruptHFileException("injected fault");
              }
              String fName = MobUtils.getMobFileName(c);
              Path pp = new Path(new Path(fs.getUri()), new Path(path, fName));
              
              // Added to support migration
              try {
                mobCell = mobStore.resolve(c, true, false);
              } catch (FileNotFoundException fnfe) {
                if (discardMobMiss) {
                  LOG.error("Missing MOB cell: file=" + pp + " not found for cell="+c);
                  continue;
                } else {
                  throw fnfe;
                }
              }
              
              if (discardMobMiss && mobCell.getValueLength() == 0) {
                LOG.error("Missing MOB cell value: file=" + pp +" cell=" + mobCell);
                continue;
              }              
              
              if (mobCell.getValueLength() > mobSizeThreshold) {
                // put the mob data back to the store file
                CellUtil.setSequenceId(mobCell, c.getSequenceId());
                mobFileWriter.append(mobCell);
                writer.append(MobUtils.createMobRefCell(mobCell, fileName, tableNameTag));
                cellsCountCompactedFromMob++;
                cellsSizeCompactedFromMob += mobCell.getValueLength();
                mobCells++;
              } else {
                // If MOB value is less than threshold, append it directly to a store file
                CellUtil.setSequenceId(mobCell, c.getSequenceId());
                writer.append(mobCell);
              }

            } else {
              // Not a MOB reference cell
              int size = c.getValueLength();
              if (size > mobSizeThreshold) {
                mobFileWriter.append(c);
                writer.append(MobUtils.createMobRefCell(c, fileName, tableNameTag));
                mobCells++;
              } else {
                writer.append(c);
              }
            }
          } else if (c.getTypeByte() != KeyValue.Type.Put.getCode()) {
            // Not a major compaction
            // If the kv type is not put, directly write the cell
            // to the store file.
            writer.append(c);
          } else if (MobUtils.isMobReferenceCell(c)) {
            // Not a major compaction, Put and MOB ref
            if (MobUtils.hasValidMobRefCellValue(c)) {
              
              // Add MOB reference to a set
              mobRefSet.get().add(MobUtils.getMobFileName(c));
              
              int size = MobUtils.getMobValueLength(c);
              if (size > mobSizeThreshold) {
                // If the value size is larger than the threshold, it's regarded as a mob. Since
                // its value is already in the mob file, directly write this cell to the store file
                writer.append(c);
              } else {
                // If the value is not larger than the threshold, it's not regarded a mob. Retrieve
                // the mob cell from the mob file, and write it back to the store file.
                mobCell = mobStore.resolve(c, true, false);                          
                if (mobCell.getValueLength() != 0) {
                  // put the mob data back to the store file
                  CellUtil.setSequenceId(mobCell, c.getSequenceId());
                  writer.append(mobCell);
                  cellsCountCompactedFromMob++;
                  cellsSizeCompactedFromMob += mobCell.getValueLength();
                } else {
                  // If the value of a file is empty, there might be issues when retrieving,
                  // directly write the cell to the store file, and leave it to be handled by the
                  // next compaction.
                  LOG.error("DDDD empty value for: " + c);
                  writer.append(c);
                }
              }
            } else {
              // TODO ????
              LOG.error("DDDD Corrupted MOB reference: " + c);
              writer.append(c);
            }
          } else if (c.getValueLength() <= mobSizeThreshold) {
            // If the value size of a cell is not larger than the threshold, directly write it to
            // the store file.
            writer.append(c);
          } else {
            // If the value size of a cell is larger than the threshold, it's regarded as a mob,
            // write this cell to a mob file, and write the path to the store file.
            mobCells++;
            // append the original keyValue in the mob file.
            mobFileWriter.append(c);
            Cell reference = MobUtils.createMobRefCell(c, fileName, tableNameTag);
            // write the cell whose value is the path of a mob file to the store file.
            writer.append(reference);
            cellsCountCompactedToMob++;
            cellsSizeCompactedToMob += c.getValueLength();
            // Add ref we get for compact MOB case
            mobRefSet.get().add(mobFileWriter.getPath().getName());
          }
          ++progress.currentCompactedKVs;

          // check periodically to see if a system stop is requested
          if (closeCheckInterval > 0) {
            bytesWritten += KeyValueUtil.length(c);
            if (bytesWritten > closeCheckInterval) {
              bytesWritten = 0;
              if (!store.areWritesEnabled()) {
                progress.cancel();
                return false;
              }
            }
          }
        }
        cells.clear();
      } while (hasMore);
    } catch (FileNotFoundException e) { 
      LOG.error("MOB Stress Test FAILED, region: "+store.getRegionInfo().getEncodedName(), e);
      System.exit(-1);
    } catch (IOException t) {
      failure = t;
      LOG.error("DDDD COMPACTION failed for region: "+ store.getRegionInfo().getEncodedName());
      throw t;
    } finally {
      if (failure == null) {
        if (mobFileWriter != null) {
          mobFileWriter.appendMetadata(fd.maxSeqId, major, mobCells);
          mobFileWriter.close();
        }
      } else {
        // Compaction failed, delete mob file in ./tmp
        if (mobFileWriter != null) {
          Path p = mobFileWriter.getPath();  
          boolean result = store.getFileSystem().delete(p, true);
          LOG.warn("DDDD Deletion of MOB file in ./tmp: "+ p + " result="+ result);
        }
        // Remove all MOB references because compaction failed
        mobRefSet.get().clear();
      }
    }
    
    if (mobFileWriter != null) {
      if (mobCells > 0) {
        // If the mob file is not empty, commit it.
        mobStore.commitFile(mobFileWriter.getPath(), path);
      } else {
        try {
          // If the mob file is empty, delete it instead of committing.
          store.getFileSystem().delete(mobFileWriter.getPath(), true);
        } catch (IOException e) {
          LOG.error("Failed to delete the temp mob file", e);
        }
      }
    }
   
    mobStore.updateCellsCountCompactedFromMob(cellsCountCompactedFromMob);
    mobStore.updateCellsCountCompactedToMob(cellsCountCompactedToMob);
    mobStore.updateCellsSizeCompactedFromMob(cellsSizeCompactedFromMob);
    mobStore.updateCellsSizeCompactedToMob(cellsSizeCompactedToMob);
    progress.complete();
    return true;
  }
  

}
