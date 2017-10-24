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

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;

import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import static java.nio.charset.StandardCharsets.UTF_8;

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
        assertUpdate("CREATE TABLE test_create_table_as_if_not_exists (created_date timestamp, a bigint, b double, c varchar(10), d varchar(10)) with(rowkeys = ARRAY['created_date row_timestamp','a', 'b', 'c'], SALT_BUCKETS=10)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create_table_as_if_not_exists"));
        assertTableColumnNames("test_create_table_as_if_not_exists", "created_date", "a", "b", "c", "d");
    }

    @Test
    public void createTableWithEveryType()
            throws Exception
    {
        @Language("SQL")
        String query = "" +
                "CREATE TABLE test_types_table AS " +
                "SELECT" +
                " 'foo' col_varchar" +
                ", cast('bar' as varbinary) col_varbinary" +
                ", cast(1 as bigint) col_bigint" +
                ", 2 col_integer" +
                ", CAST('3.14' AS DOUBLE) col_double" +
                ", true col_boolean" +
                ", DATE '1980-05-07' col_date" +
                ", TIMESTAMP '1980-05-07 11:22:33.456' col_timestamp" +
                ", CAST('3.14' AS DECIMAL(3,2)) col_decimal_short" +
                ", CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) col_decimal_long" +
                ", CAST('bar' AS CHAR(10)) col_char";

        assertUpdate(query, 1);

        MaterializedResult results = getQueryRunner().execute(getSession(), "SELECT * FROM test_types_table where CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) = col_decimal_long").toJdbcTypes();
        assertEquals(results.getRowCount(), 1);
        MaterializedRow row = results.getMaterializedRows().get(0);
        assertEquals(row.getField(0), "foo");
        assertEquals(row.getField(1), "bar".getBytes(UTF_8));
        assertEquals(row.getField(2), 1L);
        assertEquals(row.getField(3), 2);
        assertEquals(row.getField(4), 3.14);
        assertEquals(row.getField(5), true);
        assertEquals(row.getField(6), new Date(new DateTime(1980, 5, 7, 0, 0, 0, UTC).getMillis()));
        assertEquals(row.getField(7), new Timestamp(new DateTime(1980, 5, 7, 11, 22, 33, 456, UTC).getMillis()));
        assertEquals(row.getField(8), new BigDecimal("3.14"));
        assertEquals(row.getField(9), new BigDecimal("12345678901234567890.0123456789"));
        assertEquals(row.getField(10), "bar       ");
        assertUpdate("DROP TABLE test_types_table");

        assertFalse(getQueryRunner().tableExists(getSession(), "test_types_table"));
    }
}
