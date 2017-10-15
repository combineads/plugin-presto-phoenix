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

import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

@Test
public class TestPhoenixIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{

    public TestPhoenixIntegrationSmokeTest()
            throws Exception
    {
        super(() -> PhoenixQueryRunner.createPhoenixQueryRunner(ImmutableMap.of()));
    }

    @Test
    public void testCreateTableWithProperties()
    {
        assertUpdate("CREATE TABLE test_create_table_as_if_not_exists (a bigint, b double, c varchar(10), d varchar(10)) with(rowkeys = 'b,a,c', table_options = 'SALT_BUCKETS=10, DATA_BLOCK_ENCODING=\"DIFF\", TTL=FOREVER')");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create_table_as_if_not_exists"));
        assertTableColumnNames("test_create_table_as_if_not_exists", "a", "b", "c", "d");
    }
}
