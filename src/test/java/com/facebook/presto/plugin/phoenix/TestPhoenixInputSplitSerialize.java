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
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.phoenix.mapreduce.PhoenixInputSplit;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.testng.Assert.assertEquals;

public class TestPhoenixInputSplitSerialize
{
    @Test
    public void testSerialize() throws IOException
    {
        List<Scan> scans = new ArrayList<>();
        scans.add(new Scan("0".getBytes(), "1".getBytes()));
        scans.add(new Scan("2".getBytes(), "3".getBytes()));
        scans.add(new Scan("4".getBytes(), new ValueFilter(CompareFilter.CompareOp.NO_OP, new BinaryComparator(Bytes.toBytes("testValueOne")))));

        PhoenixInputSplit split = new PhoenixInputSplit(scans, 15000, "localhost");
        DataOutputBuffer out = new DataOutputBuffer();
        split.write(out);
        String stringData = Base64.getEncoder().encodeToString(out.getData());

        DataInputBuffer in = new DataInputBuffer();
        byte[] byteData = Base64.getDecoder().decode(stringData);
        in.reset(byteData, byteData.length);

        PhoenixInputSplit deserializedSplit = new PhoenixInputSplit();
        deserializedSplit.readFields(in);

        assertEquals(split, deserializedSplit);
    }
}
