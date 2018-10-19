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
 * Writes Throughput calculations and latency calculation in the csv file.
 */

public class BothFileWriting implements Runnable {
    private final Logger log = Logger.getLogger(BothFileWriting.class);
    private long firstTupleTime;
    private int recordWindowSize;
    private long windowExpiredTime;
    private long timeSpentInWindow;
    private long eventCountTotal;
    private long windowEventCount;
    private Writer fstream;
    private long totalWindowEventTime;
    private long totalTimeSpent;
    private Histogram histogram;
    private Histogram windowHistogram;

    /**
     * Constructor of file writing task
     *
     * @param firstTupleTime time when the first event started to getting processed
     * @param recordWindowSize size of record window
     * @param totalEventCount total event count processed
     * @param windowEventCount event count processed within the window
     * @param windowExpiredTime time at which window of length 'recordWindowSize' expired
     * @param timeSpentInWindow time spend in the window
     * @param fstream file stream for output file
     * @param totalWindowEventTime sum of time spent for each event belongs to the current window
     * @param totalTimeSpent total time spent for all the events
     * @param histogram histogram for complete process
     * @param windowHistogram histogram for window
     */
    public BothFileWriting(long firstTupleTime, int recordWindowSize, long totalEventCount, long
            windowEventCount, long windowExpiredTime, long timeSpentInWindow, Writer fstream, long totalWindowEventTime,
                           long totalTimeSpent, Histogram histogram, Histogram windowHistogram) {
        this.firstTupleTime = firstTupleTime;
        this.recordWindowSize = recordWindowSize;
        this.eventCountTotal = totalEventCount;
        this.windowEventCount = windowEventCount;
        this.windowExpiredTime = windowExpiredTime;
        this.timeSpentInWindow = timeSpentInWindow;
        this.fstream = fstream;
        this.totalWindowEventTime = totalWindowEventTime;
        this.totalTimeSpent = totalTimeSpent;
        this.histogram = histogram;
        this.windowHistogram = windowHistogram;
    }

    @Override public void run() {
        try {
            fstream.write(
                    (eventCountTotal / recordWindowSize) + "," +
                            ((windowEventCount * 1000) / timeSpentInWindow) + "," +
                            (eventCountTotal * 1000 / (windowExpiredTime - firstTupleTime)) + "," +
                            ((windowExpiredTime - firstTupleTime) / 1000f) + "," +
                            eventCountTotal + "," +
                            windowExpiredTime + "," +
                            ((totalWindowEventTime * 1.0) / windowEventCount) + "," +
                            ((totalTimeSpent * 1.0) / eventCountTotal) + "," +
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
