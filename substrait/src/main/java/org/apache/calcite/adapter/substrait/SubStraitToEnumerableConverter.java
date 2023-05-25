/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.substrait;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.util.BuiltInMethod;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;
import javax.sql.DataSource;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

import static java.util.Objects.requireNonNull;

/**
 * Relational expression representing a scan of a table in a SubStrait data source.
 */
public class SubStraitToEnumerableConverter
    extends ConverterImpl
    implements EnumerableRel {
  protected SubStraitToEnumerableConverter(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input) {
    super(cluster, ConventionTraitDef.INSTANCE, traits, input);
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new SubStraitToEnumerableConverter(
        getCluster(), traitSet, sole(inputs));
  }

  @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    RelOptCost cost = super.computeSelfCost(planner, mq);
    if (cost == null) {
      return null;
    }
    return cost.multiplyBy(.1);
  }

  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    return null;
  }

  private static List<ConstantExpression> toIndexesTableExpression(SqlString sqlString) {
    return requireNonNull(sqlString.getDynamicParameters(),
        () -> "sqlString.getDynamicParameters() is null for " + sqlString).stream()
        .map(Expressions::constant)
        .collect(Collectors.toList());
  }

  private static UnaryExpression getTimeZoneExpression(
      EnumerableRelImplementor implementor) {
    return Expressions.convert_(
        Expressions.call(
            implementor.getRootExpression(),
            "get",
            Expressions.constant("timeZone")),
        TimeZone.class);
  }

  private static void generateGet(EnumerableRelImplementor implementor,
      PhysType physType, BlockBuilder builder, ParameterExpression resultSet_,
      int i, Expression target, @Nullable Expression calendar_,
      SqlDialect.CalendarPolicy calendarPolicy) {
    final Primitive primitive = Primitive.ofBoxOr(physType.fieldClass(i));
    final RelDataType fieldType =
        physType.getRowType().getFieldList().get(i).getType();
    final List<Expression> dateTimeArgs = new ArrayList<>();
    dateTimeArgs.add(Expressions.constant(i + 1));
    SqlTypeName sqlTypeName = fieldType.getSqlTypeName();
    boolean offset = false;
    switch (calendarPolicy) {
    case LOCAL:
      assert calendar_ != null : "calendar must not be null";
      dateTimeArgs.add(calendar_);
      break;
    case NULL:
      // We don't specify a calendar at all, so we don't add an argument and
      // instead use the version of the getXXX that doesn't take a Calendar
      break;
    case DIRECT:
      sqlTypeName = SqlTypeName.ANY;
      break;
    case SHIFT:
      switch (sqlTypeName) {
      case TIMESTAMP:
      case DATE:
        offset = true;
        break;
      default:
        break;
      }
      break;
    default:
      break;
    }
    final Expression source;
    switch (sqlTypeName) {
    case DATE:
    case TIME:
    case TIMESTAMP:
      source =
          Expressions.call(
              getMethod(sqlTypeName, fieldType.isNullable(), offset),
              Expressions.<Expression>list()
                  .append(
                      Expressions.call(resultSet_,
                          getMethod2(sqlTypeName), dateTimeArgs))
                  .appendIf(offset, getTimeZoneExpression(implementor)));
      break;
    case ARRAY:
      final Expression x =
          Expressions.convert_(
              Expressions.call(resultSet_, jdbcGetMethod(primitive),
                  Expressions.constant(i + 1)),
              java.sql.Array.class);
      source = null;
      break;
    case NULL:
      source = RexImpTable.NULL_EXPR;
      break;
    default:
      source =
          Expressions.call(resultSet_, jdbcGetMethod(primitive),
              Expressions.constant(i + 1));
    }
    builder.add(
        Expressions.statement(
            Expressions.assign(
                target, source)));

    // [CALCITE-596] If primitive type columns contain null value, returns null
    // object
    if (primitive != null) {
      builder.add(
          Expressions.ifThen(
              Expressions.call(resultSet_, "wasNull"),
              Expressions.statement(
                  Expressions.assign(target,
                      Expressions.constant(null)))));
    }
  }

  private static Method getMethod(SqlTypeName sqlTypeName, boolean nullable,
      boolean offset) {
    switch (sqlTypeName) {
    case DATE:
      return (nullable
          ? (offset
          ? BuiltInMethod.DATE_TO_INT_OPTIONAL_OFFSET
          : BuiltInMethod.DATE_TO_INT_OPTIONAL)
          : (offset
              ? BuiltInMethod.DATE_TO_INT_OFFSET
              : BuiltInMethod.DATE_TO_INT)).method;
    case TIME:
      return (nullable
          ? BuiltInMethod.TIME_TO_INT_OPTIONAL
          : BuiltInMethod.TIME_TO_INT).method;
    case TIMESTAMP:
      return (nullable
          ? (offset
          ? BuiltInMethod.TIMESTAMP_TO_LONG_OPTIONAL_OFFSET
          : BuiltInMethod.TIMESTAMP_TO_LONG_OPTIONAL)
          : (offset
              ? BuiltInMethod.TIMESTAMP_TO_LONG_OFFSET
              : BuiltInMethod.TIMESTAMP_TO_LONG)).method;
    default:
      throw new AssertionError(sqlTypeName + ":" + nullable);
    }
  }

  private static Method getMethod2(SqlTypeName sqlTypeName) {
    switch (sqlTypeName) {
    case DATE:
      return BuiltInMethod.RESULT_SET_GET_DATE2.method;
    case TIME:
      return BuiltInMethod.RESULT_SET_GET_TIME2.method;
    case TIMESTAMP:
      return BuiltInMethod.RESULT_SET_GET_TIMESTAMP2.method;
    default:
      throw new AssertionError(sqlTypeName);
    }
  }

  /** E,g, {@code jdbcGetMethod(int)} returns "getInt". */
  private static String jdbcGetMethod(@Nullable Primitive primitive) {
    return primitive == null
        ? "getObject"
        : "get" + SqlFunctions.initcap(castNonNull(primitive.primitiveName));
  }

  private SqlString generateSql(SqlDialect dialect) {
    final SubStraitImplementor subStraitImplementor =
        new SubStraitImplementor(dialect,
            (JavaTypeFactory) getCluster().getTypeFactory());
    final SubStraitImplementor.Result result =
        subStraitImplementor.visitRoot(this.getInput());
    return result.asStatement().toSqlString(dialect);
  }
}
