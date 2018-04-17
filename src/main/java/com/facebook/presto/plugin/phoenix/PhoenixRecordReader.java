/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.phoenix;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.iterate.ConcatResultIterator;
import org.apache.phoenix.iterate.LookAheadResultIterator;
import org.apache.phoenix.iterate.MapReduceParallelScanGrouper;
import org.apache.phoenix.iterate.PeekingResultIterator;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.iterate.RoundRobinResultIterator;
import org.apache.phoenix.iterate.SequenceResultIterator;
import org.apache.phoenix.iterate.TableResultIterator;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.mapreduce.PhoenixInputSplit;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class PhoenixRecordReader
{
    private final QueryPlan queryPlan;

    public PhoenixRecordReader(final QueryPlan queryPlan) throws IOException
    {
        this.queryPlan = queryPlan;
    }

    public PhoenixResultSet getResultSet(PhoenixInputSplit split) throws Exception
    {
        final List<Scan> scans = split.getScans();

        List<PeekingResultIterator> iterators = new LinkedList<>();
        for (Scan scan : scans) {
            scan.setAttribute(BaseScannerRegionObserver.SKIP_REGION_BOUNDARY_CHECK, Bytes.toBytes(true));
            final TableResultIterator tableResultIterator = new TableResultIterator(
                    queryPlan.getContext().getConnection().getMutationState(),
                    scan,
                    null,
                    queryPlan.getContext().getConnection().getQueryServices().getRenewLeaseThresholdMilliSeconds(),
                    queryPlan,
                    MapReduceParallelScanGrouper.getInstance());

            PeekingResultIterator peekingResultIterator = LookAheadResultIterator.wrap(tableResultIterator);
            iterators.add(peekingResultIterator);
        }
        ResultIterator iterator = queryPlan.useRoundRobinIterator()
                ? RoundRobinResultIterator.newIterator(iterators, queryPlan)
                : ConcatResultIterator.newIterator(iterators);
        if (queryPlan.getContext().getSequenceManager().getSequenceCount() > 0) {
            iterator = new SequenceResultIterator(iterator, queryPlan.getContext()
                    .getSequenceManager());
        }

        return new PhoenixResultSet(iterator, queryPlan.getProjector()
                .cloneIfNecessary(),
                queryPlan.getContext());
    }
}
