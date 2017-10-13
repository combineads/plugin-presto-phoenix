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
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import com.facebook.presto.tests.H2QueryRunner;
import com.facebook.presto.tests.QueryAssertions;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multisets;
import com.google.common.collect.Multiset.Entry;
import io.airlift.units.Duration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.plugin.phoenix.PhoenixQueryRunner.createPhoenixQueryRunner;
import static com.facebook.presto.plugin.phoenix.PhoenixTestingUtils.execute;
import static com.facebook.presto.plugin.phoenix.PhoenixTestingUtils.executeUpdate;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.tpch.TpchTable.ORDERS;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

@Test
public class TestPhoenixIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private final TestingPhoenixServer phoenixServer;

    public TestPhoenixIntegrationSmokeTest()
            throws Exception
    {
        this(new TestingPhoenixServer("tpch"));
    }

    public TestPhoenixIntegrationSmokeTest(TestingPhoenixServer phoenixServer)
            throws Exception
    {
        super(() -> createPhoenixQueryRunner(phoenixServer, ORDERS));
        this.phoenixServer = phoenixServer;
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws IOException
    {
        phoenixServer.close();
    }

    @Test
    public void testSelectAll()
            throws Exception
    {
        String sql = "SELECT * FROM ORDERS";

        QueryRunner actualQueryRunner = queryRunner;
        Session session = getSession();
        String actual = sql;
        // H2QueryRunner h2QueryRunner = h2QueryRunner;
        String expected  = sql;
        boolean ensureOrdering = false;
        boolean compareUpdate = false;
        long start = System.nanoTime();
        MaterializedResult actualResults = null;
        try {
            actualResults = actualQueryRunner.execute(session, actual).toJdbcTypes();
        }
        catch (RuntimeException ex) {
            fail("Execution of 'actual' query failed: " + actual, ex);
        }
        Duration actualTime = nanosSince(start);

        long expectedStart = System.nanoTime();
        MaterializedResult expectedResults = null;
        try {
            expectedResults = h2QueryRunner.execute(session, expected, actualResults.getTypes());
        }
        catch (RuntimeException ex) {
            fail("Execution of 'expected' query failed: " + expected, ex);
        }

        if (actualResults.getUpdateType().isPresent() || actualResults.getUpdateCount().isPresent()) {
            if (!actualResults.getUpdateType().isPresent()) {
                fail("update count present without update type for query: \n" + actual);
            }
            if (!compareUpdate) {
                fail("update type should not be present (use assertUpdate) for query: \n" + actual);
            }
        }

        List<MaterializedRow> actualRows = actualResults.getMaterializedRows();
        List<MaterializedRow> expectedRows = expectedResults.getMaterializedRows();

        ImmutableMultiset<?> actualSet = ImmutableMultiset.copyOf(actualRows);
        ImmutableMultiset<?> expectedSet = ImmutableMultiset.copyOf(expectedRows);
        if (!actualSet.equals(expectedSet)) {
            for (Entry<?> entry : actualSet.entrySet()) {
                if (expectedSet.count(entry.getElement()) != entry.getCount()) {
                    System.out.println(actualSet.count(entry.getElement()));
                    System.out.println(entry.getCount());
                    System.out.println(entry.getElement().hashCode());
                    System.out.println(entry.getElement());
                }
              }
        }
    }

    @Test
    public void testColumnsInReverseOrder()
            throws Exception
    {
        String sql = "SELECT shippriority, TRIM(clerk), totalprice FROM ORDERS";

        QueryRunner actualQueryRunner = queryRunner;
        Session session = getSession();
        String actual = sql;
        // H2QueryRunner h2QueryRunner = h2QueryRunner;
        String expected  = sql;
        boolean ensureOrdering = false;
        boolean compareUpdate = false;
        long start = System.nanoTime();
        MaterializedResult actualResults = null;
        try {
            actualResults = actualQueryRunner.execute(session, actual).toJdbcTypes();
        }
        catch (RuntimeException ex) {
            fail("Execution of 'actual' query failed: " + actual, ex);
        }
        Duration actualTime = nanosSince(start);

        long expectedStart = System.nanoTime();
        MaterializedResult expectedResults = null;
        try {
            expectedResults = h2QueryRunner.execute(session, expected, actualResults.getTypes());
        }
        catch (RuntimeException ex) {
            fail("Execution of 'expected' query failed: " + expected, ex);
        }

        if (actualResults.getUpdateType().isPresent() || actualResults.getUpdateCount().isPresent()) {
            if (!actualResults.getUpdateType().isPresent()) {
                fail("update count present without update type for query: \n" + actual);
            }
            if (!compareUpdate) {
                fail("update type should not be present (use assertUpdate) for query: \n" + actual);
            }
        }

        List<MaterializedRow> actualRows = actualResults.getMaterializedRows();
        List<MaterializedRow> expectedRows = expectedResults.getMaterializedRows();

        ImmutableMultiset<?> actualSet = ImmutableMultiset.copyOf(actualRows);
        ImmutableMultiset<?> expectedSet = ImmutableMultiset.copyOf(expectedRows);
        if (!actualSet.equals(expectedSet)) {
            for (Entry<?> entry : actualSet.entrySet()) {
                if (expectedSet.count(entry.getElement()) != entry.getCount()) {
                    System.out.println(actualSet.count(entry.getElement()));
                    System.out.println(entry.getCount());
                    System.out.println(entry.getElement().hashCode());
                    System.out.println(entry.getElement());
                }
              }
        }
    }

    @Test
    public void testInsert()
            throws Exception
    {
        execute(phoenixServer, "CREATE TABLE tpch.test_insert (x bigint not null primary key, y varchar(100))");
        assertUpdate("INSERT INTO test_insert VALUES (123, 'test')", 1);
        assertQuery("SELECT * FROM test_insert", "SELECT 123 x, 'test' y");
        assertUpdate("DROP TABLE test_insert");
    }

    @Test
    public void testPhoenixTinyint1()
            throws Exception
    {
        execute(phoenixServer, "CREATE TABLE tpch.phoenix_test_tinyint1 (c_tinyint tinyint(1) not null primary key)");

        MaterializedResult actual = computeActual("SHOW COLUMNS FROM phoenix_test_tinyint1");
        MaterializedResult expected = MaterializedResult.resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("c_tinyint", "tinyint", "", "")
                .build();

        assertEquals(actual, expected);

        executeUpdate(phoenixServer, "UPSERT INTO tpch.phoenix_test_tinyint1 VALUES (127)");
        executeUpdate(phoenixServer, "UPSERT INTO tpch.phoenix_test_tinyint1 VALUES (-128)");
        MaterializedResult materializedRows = computeActual("SELECT * FROM tpch.phoenix_test_tinyint1 WHERE c_tinyint = 127");
        assertEquals(materializedRows.getRowCount(), 1);
        MaterializedRow row = getOnlyElement(materializedRows);

        assertEquals(row.getFields().size(), 1);
        assertEquals(row.getField(0), (byte) 127);

        assertUpdate("DROP TABLE phoenix_test_tinyint1");
    }
}
