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

import com.facebook.presto.tests.AbstractTestQueries;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;

import static com.facebook.presto.plugin.phoenix.PhoenixQueryRunner.createPhoenixQueryRunner;

@Test
public class TestPhoenixDistributedQueries
        extends AbstractTestQueries
{
    private final TestingPhoenixServer phoenixServer;

    public TestPhoenixDistributedQueries()
            throws Exception
    {
        this(new TestingPhoenixServer("tpch"));
    }

    public TestPhoenixDistributedQueries(TestingPhoenixServer phoenixServer)
            throws Exception
    {
        super(() -> createPhoenixQueryRunner(phoenixServer, TpchTable.getTables()));
        this.phoenixServer = phoenixServer;
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws IOException
    {
        phoenixServer.close();
    }
}
