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
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.tpch.TpchTable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.airlift.units.Duration.nanosSince;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;

public abstract class PhoenixTestingUtils
{
    private static final Logger log = Logger.get(PhoenixQueryRunner.class);

    private PhoenixTestingUtils()
    {
    }

    public static void copyTpchTables(
            TestingPhoenixServer phoenixServer,
            QueryRunner queryRunner,
            String sourceCatalog,
            String sourceSchema,
            Session session,
            Iterable<TpchTable<?>> tables)
    {
        log.info("Loading data from %s.%s...", sourceCatalog, sourceSchema);
        long startTime = System.nanoTime();
        for (TpchTable<?> table : tables) {
            String tableName = table.getTableName().toLowerCase(ENGLISH);
            TPCH_CREATE_STATEMENTS.get(tableName).stream().forEach(sql -> execute(phoenixServer, sql));
            copyTable(queryRunner, new QualifiedObjectName(sourceCatalog, sourceSchema, tableName), session);
        }
        log.info("Loading from %s.%s complete in %s", sourceCatalog, sourceSchema, nanosSince(startTime).toString(SECONDS));
    }

    public static void copyTable(QueryRunner queryRunner, QualifiedObjectName table, Session session)
    {
        long start = System.nanoTime();
        log.info("Running import for %s", table.getObjectName());
        String sql = format("INSERT INTO %s SELECT * FROM %s", table.getObjectName(), table);
        long rows = (Long) queryRunner.execute(session, sql).getMaterializedRows().get(0).getField(0);
        log.info("Imported %s rows for %s in %s", rows, table.getObjectName(), nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    public static void createSchema(TestingPhoenixServer phoenixServer, String schema) throws SQLException
    {
        execute(phoenixServer, format("CREATE SCHEMA %s", schema));
    }

    public static void execute(TestingPhoenixServer phoenixServer, String sql)
    {
        try (Connection connection = DriverManager.getConnection(phoenixServer.getJdbcUrl());
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (Exception e) {
            throw new IllegalStateException("Can't execute sql.", e);
        }
    }

    public static int executeUpdate(TestingPhoenixServer phoenixServer, String sql)
    {
        try (Connection connection = DriverManager.getConnection(phoenixServer.getJdbcUrl());
                Statement statement = connection.createStatement()) {
            int updatedCount = statement.executeUpdate(sql);
            connection.commit();
            return updatedCount;
        }
        catch (Exception e) {
            throw new IllegalStateException("Can't execute sql.", e);
        }
    }

    private static final String CUSTOMER_SQL = "CREATE TABLE TPCH.CUSTOMER (" +
            "    CUSTKEY BIGINT NOT NULL PRIMARY KEY," +
            "    NAME VARCHAR(25)," +
            "    ADDRESS VARCHAR(40)," +
            "    NATIONKEY BIGINT," +
            "    PHONE VARCHAR(15)," +
            "    ACCTBAL DECIMAL(15,2)," +
            "    MKTSEGMENT VARCHAR(10)," +
            "    COMMENT VARCHAR(117)" +
            " )";

    private static final String LINEITEM_SQL = "CREATE TABLE TPCH.LINEITEM (" +
            "    ORDERKEY BIGINT NOT NULL," +
            "    PARTKEY BIGINT," +
            "    SUPPKEY BIGINT," +
            "    LINENUMBER INTEGER NOT NULL," +
            "    QUANTITY DECIMAL(15,2)," +
            "    EXTENDEDPRICE DECIMAL(15,2)," +
            "    DISCOUNT DECIMAL(15,2)," +
            "    TAX DECIMAL(15,2)," +
            "    RETURNFLAG VARCHAR(1)," +
            "    LINESTATUS VARCHAR(1)," +
            "    SHIPDATE DATE," +
            "    COMMITDATE DATE," +
            "    RECEIPTDATE DATE," +
            "    SHIPINSTRUCT VARCHAR(25)," +
            "    SHIPMODE VARCHAR(10)," +
            "    COMMENT VARCHAR(44)" +
            "    CONSTRAINT PK PRIMARY KEY (ORDERKEY, LINENUMBER) " +
            " )";

    private static final String LINEITEM_L_SHIPDATE_IDX_SQL = "CREATE INDEX L_SHIPDATE_IDX on TPCH.LINEITEM( SHIPDATE, PARTKEY, EXTENDEDPRICE, DISCOUNT) ASYNC";
    private static final String LINEITEM_L_PART_IDX_SQL = "CREATE INDEX L_PART_IDX on TPCH.LINEITEM( PARTKEY, ORDERKEY, SUPPKEY, SHIPDATE, EXTENDEDPRICE, DISCOUNT, QUANTITY, SHIPMODE, SHIPINSTRUCT) ASYNC";

    private static final String NATION_SQL = "CREATE TABLE TPCH.NATION (" +
            "    NATIONKEY BIGINT NOT NULL PRIMARY KEY," +
            "    NAME VARCHAR(25)," +
            "    REGIONKEY BIGINT," +
            "    COMMENT VARCHAR(152)" +
            " )";

    private static final String ORDERS_SQL = "CREATE TABLE TPCH.ORDERS (" +
            "    ORDERKEY BIGINT NOT NULL PRIMARY KEY," +
            "    CUSTKEY BIGINT," +
            "    ORDERSTATUS VARCHAR(1)," +
            "    TOTALPRICE DECIMAL(15,2)," +
            "    ORDERDATE DATE," +
            "    ORDERPRIORITY VARCHAR(15)," +
            "    CLERK VARCHAR(15)," +
            "    SHIPPRIORITY INTEGER," +
            "    COMMENT VARCHAR(79)" +
            " )";

    private static final String ORDERS_O_CUST_IDX_SQL = "CREATE INDEX O_CUST_IDX on TPCH.ORDERS( CUSTKEY, ORDERKEY)";

    private static final String ORDERS_O_DATE_PRI_KEY_IDX_SQL = "CREATE INDEX O_DATE_PRI_KEY_IDX on TPCH.ORDERS( ORDERDATE, ORDERPRIORITY, ORDERKEY)";

    private static final String PART_SQL = "CREATE TABLE TPCH.PART (" +
            "    PARTKEY BIGINT NOT NULL PRIMARY KEY," +
            "    NAME VARCHAR(55)," +
            "    MFGR VARCHAR(25)," +
            "    BRAND VARCHAR(10)," +
            "    TYPE VARCHAR(25)," +
            "    SIZE INTEGER," +
            "    CONTAINER VARCHAR(10)," +
            "    RETAILPRICE DECIMAL(15,2)," +
            "    COMMENT VARCHAR(23)" +
            " )";

    private static final String PARTSUPP_SQL = "CREATE TABLE TPCH.PARTSUPP (" +
            "    PARTKEY BIGINT NOT NULL," +
            "    SUPPKEY BIGINT NOT NULL," +
            "    AVAILQTY INTEGER," +
            "    SUPPLYCOST DECIMAL(15,2)," +
            "    COMMENT VARCHAR(199)" +
            "    CONSTRAINT PK PRIMARY KEY (PARTKEY, SUPPKEY)" +
            " )";

    private static final String REGION_SQL = "CREATE TABLE TPCH.REGION (" +
            "    REGIONKEY BIGINT NOT NULL PRIMARY KEY," +
            "    NAME VARCHAR(25)," +
            "    COMMENT VARCHAR(152)" +
            " )";

    private static final String SUPPLIER_SQL = "CREATE TABLE TPCH.SUPPLIER (" +
            "    SUPPKEY BIGINT NOT NULL PRIMARY KEY," +
            "    NAME VARCHAR(25)," +
            "    ADDRESS VARCHAR(40)," +
            "    NATIONKEY BIGINT," +
            "    PHONE VARCHAR(15)," +
            "    ACCTBAL DECIMAL(15,2)," +
            "    COMMENT VARCHAR(101)" +
            " )";

    private static final Map<String, List<String>> TPCH_CREATE_STATEMENTS = new ImmutableMap.Builder<String, List<String>>()
            .put("customer", Arrays.asList(CUSTOMER_SQL))
            .put("lineitem", Arrays.asList(LINEITEM_SQL, LINEITEM_L_SHIPDATE_IDX_SQL, LINEITEM_L_PART_IDX_SQL))
            .put("nation", Arrays.asList(NATION_SQL))
            .put("orders", Arrays.asList(ORDERS_SQL, ORDERS_O_CUST_IDX_SQL, ORDERS_O_DATE_PRI_KEY_IDX_SQL))
            .put("part", Arrays.asList(PART_SQL))
            .put("partsupp", Arrays.asList(PARTSUPP_SQL))
            .put("region", Arrays.asList(REGION_SQL))
            .put("supplier", Arrays.asList(SUPPLIER_SQL))
            .build();
}
