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

import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.session.PropertyMetadata.stringSessionProperty;

import static java.util.Objects.requireNonNull;

/**
 * Class contains all table properties for the Phoenix connector. Used when creating a table:
 * <p>
 * CREATE TABLE foo (a VARCHAR, b INT)
 * WITH (rowkeys = 'a,b', table_options = 'SALT_BUCKETS=10, DATA_BLOCK_ENCODING="DIFF"');
 */
public final class PhoenixTableProperties
{
    public static final String ROWKEYS = "rowkeys";
    public static final String TABLE_OPTIONS = "table_options";
    private static final Splitter COMMA_SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();

    private final List<PropertyMetadata<?>> tableProperties;

    public PhoenixTableProperties()
    {
        PropertyMetadata<String> s1 = stringSessionProperty(
                ROWKEYS,
                "The list of columns to be the primary key in a Phoenix table.",
                null,
                false);

        PropertyMetadata<String> s2 = stringSessionProperty(
                TABLE_OPTIONS,
                "The list of columns to be the table options in a Phoenix table.",
                null,
                false);

        tableProperties = ImmutableList.of(s1, s2);
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public static Optional<List<String>> getRowkeys(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties);

        String strRowkeys = (String) tableProperties.get(ROWKEYS);
        if (strRowkeys == null) {
            return Optional.empty();
        }

        ImmutableList.Builder<String> rowkeys = ImmutableList.builder();
        for (String rowkey : COMMA_SPLITTER.split(strRowkeys)) {
            rowkeys.add(rowkey);
        }
        return Optional.of(rowkeys.build());
    }

    public static Optional<String> getTableOptions(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties);

        String tableOptions = (String) tableProperties.get(TABLE_OPTIONS);
        return Optional.ofNullable(tableOptions);
    }
}
