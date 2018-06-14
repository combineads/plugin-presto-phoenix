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

@Test(singleThreaded = true)
public class TestPhoenixCacheTest
        extends AbstractTestIntegrationSmokeTest
{
    public TestPhoenixCacheTest()
            throws Exception
    {
        super(() -> PhoenixQueryRunner.createPhoenixQueryRunner(4, ImmutableMap.of(), ImmutableList.of()));
    }

    @Test(invocationCount = 5)
    public void tesdtMetaCacahe() throws InterruptedException
    {
        String tableName = "TEST_META_CACHE";
        assertUpdate("CREATE TABLE " + tableName + "(ENTRY VARCHAR, DUMMY1 VARCHAR, DUMMY2 VARCHAR) WITH (ROWKEYS = ARRAY['ENTRY'], UPDATE_CACHE_FREQUENCY=0)");
        assertUpdate("INSERT INTO " + tableName + " VALUES('KEY1', 'DUMMYVALUE1', '3000')", 1);
        assertQuery("SELECT ENTRY, DUMMY1, DUMMY2 FROM " + tableName + " where ENTRY = 'KEY1'", "SELECT 'KEY1', 'DUMMYVALUE1', '3000'");
        assertUpdate("DROP TABLE " + tableName + "");

        assertUpdate("CREATE TABLE " + tableName + "(ENTRY VARCHAR, DUMMY1 BIGINT, DUMMY2 BIGINT) WITH (ROWKEYS = ARRAY['ENTRY'], UPDATE_CACHE_FREQUENCY=0)");
        assertUpdate("INSERT INTO " + tableName + " VALUES('KEY1', 2000, 3000)", 1);
        assertQuery("SELECT ENTRY, DUMMY1, DUMMY2 FROM " + tableName + " where ENTRY = 'KEY1'", "SELECT 'KEY1', 2000, 3000");
        assertUpdate("DROP TABLE " + tableName + "");
    }

    @Test(invocationCount = 5)
    public void tesdtDifferentColumnNameMetaCacahe() throws InterruptedException
    {
        String tableName = "TEST_META_CACHE";
        assertUpdate("CREATE TABLE " + tableName + "(ENTRY VARCHAR, DUMMY1 VARCHAR, DUMMY2 VARCHAR) WITH (ROWKEYS = ARRAY['ENTRY'], UPDATE_CACHE_FREQUENCY=0)");
        assertUpdate("INSERT INTO " + tableName + " VALUES('KEY1', 'DUMMYVALUE1', '3000')", 1);
        assertQuery("SELECT ENTRY, DUMMY1, DUMMY2 FROM " + tableName + " where ENTRY = 'KEY1'", "SELECT 'KEY1', 'DUMMYVALUE1', '3000'");
        assertUpdate("DROP TABLE " + tableName + "");

        assertUpdate("CREATE TABLE " + tableName + "(ENTRY VARCHAR, DUMMY3 BIGINT, DUMMY4 BIGINT) WITH (ROWKEYS = ARRAY['ENTRY'], UPDATE_CACHE_FREQUENCY=0)");
        assertUpdate("INSERT INTO " + tableName + " VALUES('KEY1', 2000, 3000)", 1);
        assertQuery("SELECT ENTRY, DUMMY3, DUMMY4 FROM " + tableName + " where ENTRY = 'KEY1'", "SELECT 'KEY1', 2000, 3000");
        assertUpdate("DROP TABLE " + tableName + "");
    }

    @Test(invocationCount = 5)
    public void tesdtDynamicColumnNameMetaCacahe() throws InterruptedException
    {
        String tableName = "TEST_META_CACHE";
        String dynamicTableName = "\"TEST_META_CACHE$DCOL1 VARCHAR,DCOL2 VARCHAR\"";
        assertUpdate("CREATE TABLE " + tableName + "(ENTRY VARCHAR, DUMMY1 VARCHAR, DUMMY2 VARCHAR) WITH (ROWKEYS = ARRAY['ENTRY'], UPDATE_CACHE_FREQUENCY=0)");
        assertUpdate("INSERT INTO " + dynamicTableName + " VALUES('KEY1', 'DUMMYVALUE1', '3000', 'DCOLVALUE1', 'DCOLVALUE2')", 1);
        assertQuery("SELECT ENTRY, DUMMY1, DUMMY2, DCOL1, DCOL2 FROM " + dynamicTableName + " where ENTRY = 'KEY1'", "SELECT 'KEY1', 'DUMMYVALUE1', '3000', 'DCOLVALUE1', 'DCOLVALUE2'");
        assertUpdate("DROP TABLE " + tableName + "");

        dynamicTableName = "\"TEST_META_CACHE$DCOL1 BIGINT,DCOL2 BIGINT\"";
        assertUpdate("CREATE TABLE " + tableName + "(ENTRY VARCHAR, DUMMY3 BIGINT, DUMMY4 BIGINT) WITH (ROWKEYS = ARRAY['ENTRY'], UPDATE_CACHE_FREQUENCY=0)");
        assertUpdate("INSERT INTO " + dynamicTableName + " VALUES('KEY1', 2000, 3000, 4000, 5000)", 1);
        assertQuery("SELECT ENTRY, DUMMY3, DUMMY4, DCOL1, DCOL2 FROM " + dynamicTableName + " where ENTRY = 'KEY1'", "SELECT 'KEY1', 2000, 3000, 4000, 5000");
        assertUpdate("DROP TABLE " + tableName + "");
    }
}
