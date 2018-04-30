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

import com.facebook.presto.Session;
import com.facebook.presto.tests.AbstractTestDistributedQueries;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test
public class TestPhoenixDistributedQueries
        extends AbstractTestDistributedQueries
{
    public TestPhoenixDistributedQueries()
            throws Exception
    {
        super(() -> PhoenixQueryRunner.createPhoenixQueryRunner(ImmutableMap.of(), TpchTable.getTables()));
    }
    @Test
    public void testCreateTableAsSelect()
    {
        // First column is used the primary key of Phoenix connector.
        assertUpdate("CREATE TABLE test_create_table_as_if_not_exists (a bigint, b double)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create_table_as_if_not_exists"));
        assertTableColumnNames("test_create_table_as_if_not_exists", "a", "b");

        assertUpdate("CREATE TABLE IF NOT EXISTS test_create_table_as_if_not_exists AS SELECT orderkey, discount FROM lineitem", 0);
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create_table_as_if_not_exists"));
        assertTableColumnNames("test_create_table_as_if_not_exists", "a", "b");

        assertUpdate("DROP TABLE test_create_table_as_if_not_exists");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_create_table_as_if_not_exists"));

        assertCreateTableAsSelect(
                "test_select",
                "SELECT orderkey, orderdate, totalprice FROM orders",
                "SELECT count(*) FROM orders");

        assertCreateTableAsSelect(
                "test_group",
                "SELECT orderstatus, sum(totalprice) x FROM orders GROUP BY orderstatus",
                "SELECT count(DISTINCT orderstatus) FROM orders");

        assertCreateTableAsSelect(
                "test_join",
                "SELECT 'rowid' rowid, count(*) x FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey",
                "SELECT 1");

        assertCreateTableAsSelect(
                "test_limit",
                "SELECT orderkey FROM orders ORDER BY orderkey LIMIT 10",
                "SELECT 10");

        assertCreateTableAsSelect(
                "test_unicode",
                "SELECT '\u2603' unicode",
                "SELECT 1");

        assertCreateTableAsSelect(
                "test_with_data",
                "SELECT orderkey, orderdate, totalprice FROM orders WITH DATA",
                "SELECT orderkey, orderdate, totalprice FROM orders",
                "SELECT count(*) FROM orders");

        assertCreateTableAsSelect(
                "test_with_no_data",
                "SELECT orderkey, orderdate, totalprice FROM orders WITH NO DATA",
                "SELECT orderkey, orderdate, totalprice FROM orders LIMIT 0",
                "SELECT 0");

        // Tests for CREATE TABLE with UNION ALL: exercises PushTableWriteThroughUnion optimizer

        assertCreateTableAsSelect(
                "test_union_all",
                "SELECT orderkey, orderdate, totalprice FROM orders WHERE orderkey % 2 = 0 UNION ALL " +
                        "SELECT orderkey, orderdate, totalprice FROM orders WHERE orderkey % 2 = 1",
                "SELECT orderkey, orderdate, totalprice FROM orders",
                "SELECT count(*) FROM orders");

        assertCreateTableAsSelect(
                Session.builder(getSession()).setSystemProperty("redistribute_writes", "true").build(),
                "test_union_all",
                "SELECT orderkey, orderdate, totalprice FROM orders UNION ALL " +
                        "SELECT 1234567890, DATE '2000-01-01', 1.23",
                "SELECT orderkey, orderdate, totalprice FROM orders UNION ALL " +
                        "SELECT 1234567890, DATE '2000-01-01', 1.23",
                "SELECT count(*) + 1 FROM orders");

        assertCreateTableAsSelect(
                Session.builder(getSession()).setSystemProperty("redistribute_writes", "false").build(),
                "test_union_all",
                "SELECT orderkey, orderdate, totalprice FROM orders UNION ALL " +
                        "SELECT 1234567890, DATE '2000-01-01', 1.23",
                "SELECT  orderkey, orderdate,totalprice FROM orders UNION ALL " +
                        "SELECT 1234567890, DATE '2000-01-01', 1.23",
                "SELECT count(*) + 1 FROM orders");
    }

    @Test
    public void testInsertDuplicateRows()
    {
        // Insert a row without specifying the comment column. That column will be null.
        // https://prestodb.io/docs/current/sql/insert.html
        try {
            assertUpdate("CREATE TABLE test_insert_duplicate AS SELECT 1 a, 2 b, '3' c", 1);
            assertQuery("SELECT a, b, c FROM test_insert_duplicate", "SELECT 1, 2, '3'");
            assertUpdate("INSERT INTO test_insert_duplicate (a, c) VALUES (1, '4')", 1);
            assertQuery("SELECT a, b, c FROM test_insert_duplicate", "SELECT 1, null, '4'");
            assertUpdate("INSERT INTO test_insert_duplicate (a, b) VALUES (1, 3)", 1);
            assertQuery("SELECT a, b, c FROM test_insert_duplicate", "SELECT 1, 3, null");
        }
        finally {
            assertUpdate("DROP TABLE test_insert_duplicate");
        }
    }
}
