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

import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

@Test(singleThreaded = true)
public class TestPhoenixSnapshotTest
        extends AbstractTestQueryFramework
{
    public TestPhoenixSnapshotTest() throws Exception
    {
        super(() -> PhoenixQueryRunner.createPhoenixQueryRunner(1, ImmutableMap.of(), ImmutableList.of()));
    }

    @Test
    public void testTransactionRollback()
    {
        assertUpdate("START TRANSACTION");
        assertUpdate("CREATE TABLE TEST_DUMMY_TRANS (ENTRY VARCHAR, VAL1 BIGINT) WITH (ROWKEYS = ARRAY['ENTRY'])");
        assertUpdate("INSERT INTO TEST_DUMMY_TRANS(ENTRY, VAL1) VALUES ('ROW1', 1), ('ROW2', 2), ('ROW3', 3), ('ROW4', 4), ('ROW5', 5)", 5);
        assertUpdate("COMMIT");
        assertQuery("SELECT ENTRY, VAL1 FROM TEST_DUMMY_TRANS WHERE ENTRY = 'ROW1'", "SELECT 'ROW1', 1");
        assertUpdate("START TRANSACTION");
        assertUpdate("INSERT INTO TEST_DUMMY_TRANS(ENTRY, VAL1) VALUES ('ROW1', 2), ('ROW6', 6)", 5);
        assertUpdate("ROLLBACK");
        assertQuery("SELECT ENTRY, VAL1 FROM TEST_DUMMY_TRANS WHERE ENTRY = 'ROW1'", "SELECT 'ROW1', 1");
        assertQuery("SELECT COUNT(1) AS CNT FROM TEST_DUMMY_TRANS", "SELECT 5S");
    }
}
