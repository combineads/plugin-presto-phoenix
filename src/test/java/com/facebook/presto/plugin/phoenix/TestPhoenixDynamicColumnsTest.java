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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

@Test
public class TestPhoenixDynamicColumnsTest
        extends AbstractTestIntegrationSmokeTest
{
    public TestPhoenixDynamicColumnsTest()
            throws Exception
    {
        super(() -> PhoenixQueryRunner.createPhoenixQueryRunner(ImmutableMap.of(), ImmutableList.of()));
    }

    @Test
    public void tesdtDuplicateKeyUpdateColumns()
    {
        assertUpdate("CREATE TABLE TEST_DYNAMIC_COLUMNS (ENTRY VARCHAR, DUMMY VARCHAR, DUMMY2 VARCHAR) WITH (ROWKEYS = ARRAY['ENTRY'])");
        assertUpdate("INSERT INTO \"test_dynamic_columns$DynColA VARCHAR, DynColB varchar\" VALUES('dynEntry','DynColValuea','DynColValueb')", 1);
        assertQuery("SELECT DynColA FROM \"test_dynamic_columns$DynColA VARCHAR\" where entry='dynEntry'", "SELECT 'DynColValuea'");
        assertQuery("SELECT DynColB FROM \"test_dynamic_columns$DynColB VARCHAR\" where entry='dynEntry'", "SELECT 'DynColValueb'");
    }
}
