/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.extension.siddhi.execution.throughput.util.filewriting;

import org.HdrHistogram.Histogram;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Writer;


/**
 * Writes Latency calculations in the csv file.
 */
public class LatencyFileWriting implements Runnable {
    private final Logger log = Logger.getLogger(LatencyFileWriting.class);
    private long recordWindow;
    private long timeSpent;
    private long totalTimeSpent;
    private long eventCountTotal;
    private long eventCount;
    private Writer fstream;
    private Histogram histogram;
    private Histogram windowHistogram;

    /**
     * Constructor of file writing task
     *
     * @param recordWindowSize
     * @param totalEventCount
     * @param windowEventCount
     * @param timeSpentInWindow
     * @param totalTimeSpent
     * @param histogram
     * @param windowHistogram
     * @param fstream
     */
    public LatencyFileWriting(int recordWindowSize, long totalEventCount, long windowEventCount, long timeSpentInWindow,
                              long totalTimeSpent, Histogram histogram, Histogram windowHistogram, Writer fstream) {
        this.recordWindow = recordWindowSize;
        this.eventCountTotal = totalEventCount;
        this.eventCount = windowEventCount;
        this.timeSpent = timeSpentInWindow;
        this.totalTimeSpent = totalTimeSpent;
        this.histogram = histogram;
        this.windowHistogram = windowHistogram;
        this.fstream = fstream;
    }

    @Override public void run() {
        try {
            fstream.write(
                    ((eventCountTotal / recordWindow) + "," +
                            timeSpent * 1.0 / eventCount) + "," +
                            ((totalTimeSpent * 1.0) / eventCountTotal) + "," +
                            eventCountTotal + "," +
                            timeSpent + "," +
                            totalTimeSpent + "," +
                            histogram.getValueAtPercentile(90.0) + "," +
                            histogram.getValueAtPercentile(95.0) + "," +
                            histogram.getValueAtPercentile(99.0) + "," +
                            windowHistogram.getValueAtPercentile(90.0) + "," +
                            windowHistogram.getValueAtPercentile(95.0) + "," +
                            windowHistogram.getValueAtPercentile(99.0));
            fstream.write("\r\n");
            fstream.flush();
        } catch (IOException ex) {
            log.error("Error while writing into the file : " + ex.getMessage(), ex);
        }
    }
}
