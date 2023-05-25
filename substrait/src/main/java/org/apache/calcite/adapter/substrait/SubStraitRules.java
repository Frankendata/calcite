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

import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.*;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * Rules and relational operators for
 * {@link SubStraitConvention}
 * calling convention.
 */
public class SubStraitRules {
  private SubStraitRules() {
  }

  protected static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  static final RelFactories.ProjectFactory PROJECT_FACTORY =
      (input, hints, projects, fieldNames, variablesSet) -> {
        Preconditions.checkArgument(variablesSet.isEmpty(),
            "SubStraitProject does not allow variables");
        final RelOptCluster cluster = input.getCluster();
        final RelDataType rowType =
            RexUtil.createStructType(cluster.getTypeFactory(), projects,
                fieldNames, SqlValidatorUtil.F_SUGGESTER);
        return new SubStraitProject(cluster, input.getTraitSet(), input, projects,
            rowType);
      };

  static final RelFactories.FilterFactory FILTER_FACTORY =
      (input, condition, variablesSet) -> {
        Preconditions.checkArgument(variablesSet.isEmpty(),
            "SubStraitFilter does not allow variables");
        return new SubStraitFilter(input.getCluster(),
            input.getTraitSet(), input, condition);
      };

  static final RelFactories.JoinFactory JOIN_FACTORY =
      (left, right, hints, condition, variablesSet, joinType, semiJoinDone) -> {
        final RelOptCluster cluster = left.getCluster();
        final RelTraitSet traitSet =
            cluster.traitSetOf(
                requireNonNull(left.getConvention(), "left.getConvention()"));
        try {
          return new SubStraitJoin(cluster, traitSet, left, right, condition,
              variablesSet, joinType);
        } catch (InvalidRelException e) {
          throw new AssertionError(e);
        }
      };

  static final RelFactories.CorrelateFactory CORRELATE_FACTORY =
      (left, right, hints, correlationId, requiredColumns, joinType) -> {
        throw new UnsupportedOperationException("SubStraitCorrelate");
      };

  public static final RelFactories.SortFactory SORT_FACTORY =
      (input, collation, offset, fetch) -> {
        throw new UnsupportedOperationException("SubStraitSort");
      };

  public static final RelFactories.ExchangeFactory EXCHANGE_FACTORY =
      (input, distribution) -> {
        throw new UnsupportedOperationException("SubStraitExchange");
      };

  public static final RelFactories.SortExchangeFactory SORT_EXCHANGE_FACTORY =
      (input, distribution, collation) -> {
        throw new UnsupportedOperationException("SubStraitSortExchange");
      };

  public static final RelFactories.AggregateFactory AGGREGATE_FACTORY =
      (input, hints, groupSet, groupSets, aggCalls) -> {
        final RelOptCluster cluster = input.getCluster();
        final RelTraitSet traitSet =
            cluster.traitSetOf(
                requireNonNull(input.getConvention(), "input.getConvention()"));
        try {
          return new SubStraitAggregate(cluster, traitSet, input, groupSet,
              groupSets, aggCalls);
        } catch (InvalidRelException e) {
          throw new AssertionError(e);
        }
      };

  public static final RelFactories.MatchFactory MATCH_FACTORY =
      (input, pattern, rowType, strictStart, strictEnd, patternDefinitions,
          measures, after, subsets, allRows, partitionKeys, orderKeys,
          interval) -> {
        throw new UnsupportedOperationException("SubStraitMatch");
      };

  public static final RelFactories.SetOpFactory SET_OP_FACTORY =
      (kind, inputs, all) -> {
        RelNode input = inputs.get(0);
        RelOptCluster cluster = input.getCluster();
        final RelTraitSet traitSet =
            cluster.traitSetOf(
                requireNonNull(input.getConvention(), "input.getConvention()"));
        switch (kind) {
        case UNION:
          return new SubStraitUnion(cluster, traitSet, inputs, all);
        case INTERSECT:
          return new SubStraitIntersect(cluster, traitSet, inputs, all);
        case EXCEPT:
          return new SubStraitMinus(cluster, traitSet, inputs, all);
        default:
          throw new AssertionError("unknown: " + kind);
        }
      };

  public static final RelFactories.ValuesFactory VALUES_FACTORY =
      (cluster, rowType, tuples) -> {
        throw new UnsupportedOperationException();
      };

  public static final RelFactories.TableScanFactory TABLE_SCAN_FACTORY =
      (toRelContext, table) -> {
        throw new UnsupportedOperationException();
      };

  public static final RelFactories.SnapshotFactory SNAPSHOT_FACTORY =
      (input, period) -> {
        throw new UnsupportedOperationException();
      };

  /** A {@link RelBuilderFactory} that creates a {@link RelBuilder} that will
   * create SubStrait relational expressions for everything. */
  public static final RelBuilderFactory SubStrait_BUILDER =
      RelBuilder.proto(
          Contexts.of(PROJECT_FACTORY,
              FILTER_FACTORY,
              JOIN_FACTORY,
              SORT_FACTORY,
              EXCHANGE_FACTORY,
              SORT_EXCHANGE_FACTORY,
              AGGREGATE_FACTORY,
              MATCH_FACTORY,
              SET_OP_FACTORY,
              VALUES_FACTORY,
              TABLE_SCAN_FACTORY,
              SNAPSHOT_FACTORY));

  /** Creates a list of rules with the given SubStrait convention instance. */
  public static List<RelOptRule> rules(SubStraitConvention out) {
    final ImmutableList.Builder<RelOptRule> b = ImmutableList.builder();
    foreachRule(out, b::add);
    return b.build();
  }

  /** Creates a list of rules with the given SubStrait convention instance
   * and builder factory. */
  public static List<RelOptRule> rules(SubStraitConvention out,
                                       RelBuilderFactory relBuilderFactory) {
    final ImmutableList.Builder<RelOptRule> b = ImmutableList.builder();
    foreachRule(out, r ->
        b.add(r.config.withRelBuilderFactory(relBuilderFactory).toRule()));
    return b.build();
  }

  private static void foreachRule(SubStraitConvention out,
                                  Consumer<RelRule<?>> consumer) {
    consumer.accept(SubStraitToEnumerableConverterRule.create(out));
    consumer.accept(SubStraitJoinRule.create(out));
    consumer.accept(SubStraitProjectRule.create(out));
    consumer.accept(SubStraitFilterRule.create(out));
    consumer.accept(SubStraitAggregateRule.create(out));
    consumer.accept(SubStraitSortRule.create(out));
    consumer.accept(SubStraitUnionRule.create(out));
    consumer.accept(SubStraitIntersectRule.create(out));
    consumer.accept(SubStraitMinusRule.create(out));
    consumer.accept(SubStraitTableModificationRule.create(out));
    consumer.accept(SubStraitValuesRule.create(out));
  }

  /** Abstract base class for rule that converts to SubStrait. */
  abstract static class SubStraitConverterRule extends ConverterRule {
    protected SubStraitConverterRule(Config config) {
      super(config);
    }
  }

  /** Rule that converts a join to SubStrait. */
  public static class SubStraitJoinRule extends SubStraitConverterRule {
    /** Creates a SubStraitJoinRule. */
    public static SubStraitJoinRule create(SubStraitConvention out) {
      return Config.INSTANCE
          .withConversion(Join.class, Convention.NONE, out, "SubStraitJoinRule")
          .withRuleFactory(SubStraitJoinRule::new)
          .toRule(SubStraitJoinRule.class);
    }

    /** Called from the Config. */
    protected SubStraitJoinRule(Config config) {
      super(config);
    }

    @Override public @Nullable RelNode convert(RelNode rel) {
      final Join join = (Join) rel;
      switch (join.getJoinType()) {
      case SEMI:
      case ANTI:
        // It's not possible to convert semi-joins or anti-joins. They have fewer columns
        // than regular joins.
        return null;
      default:
        return convert(join, true);
      }
    }

    /**
     * Converts a {@code Join} into a {@code SubStraitJoin}.
     *
     * @param join Join operator to convert
     * @param convertInputTraits Whether to convert input to {@code join}'s
     *                            SubStrait convention
     * @return A new SubStraitJoin
     */
    public @Nullable RelNode convert(Join join, boolean convertInputTraits) {
      final List<RelNode> newInputs = new ArrayList<>();
      for (RelNode input : join.getInputs()) {
        if (convertInputTraits && input.getConvention() != getOutTrait()) {
          input =
              convert(input,
                  input.getTraitSet().replace(out));
        }
        newInputs.add(input);
      }
      if (convertInputTraits && !canJoinOnCondition(join.getCondition())) {
        return null;
      }
      try {
        return new SubStraitJoin(
            join.getCluster(),
            join.getTraitSet().replace(out),
            newInputs.get(0),
            newInputs.get(1),
            join.getCondition(),
            join.getVariablesSet(),
            join.getJoinType());
      } catch (InvalidRelException e) {
        LOGGER.debug(e.toString());
        return null;
      }
    }

    /**
     * Returns whether a condition is supported by {@link SubStraitJoin}.
     *
     * <p>Corresponds to the capabilities of
     * {@link SqlImplementor#convertConditionToSqlNode}.
     *
     * @param node Condition
     * @return Whether condition is supported
     */
    private static boolean canJoinOnCondition(RexNode node) {
      final List<RexNode> operands;
      switch (node.getKind()) {
      case LITERAL:
        // literal on a join condition would be TRUE or FALSE
        return true;
      case AND:
      case OR:
        operands = ((RexCall) node).getOperands();
        for (RexNode operand : operands) {
          if (!canJoinOnCondition(operand)) {
            return false;
          }
        }
        return true;

      case EQUALS:
      case IS_NOT_DISTINCT_FROM:
      case NOT_EQUALS:
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
        operands = ((RexCall) node).getOperands();
        if ((operands.get(0) instanceof RexInputRef)
            && (operands.get(1) instanceof RexInputRef)) {
          return true;
        }
        // fall through

      default:
        return false;
      }
    }

    @Override public boolean matches(RelOptRuleCall call) {
      Join join = call.rel(0);
      JoinRelType joinType = join.getJoinType();
      return false;
    }
  }

  /** Join operator implemented in SubStrait convention. */
  public static class SubStraitJoin extends Join implements SubStraitRel {
    /** Creates a SubStraitJoin. */
    public SubStraitJoin(RelOptCluster cluster, RelTraitSet traitSet,
        RelNode left, RelNode right, RexNode condition,
        Set<CorrelationId> variablesSet, JoinRelType joinType)
        throws InvalidRelException {
      super(cluster, traitSet, ImmutableList.of(), left, right, condition, variablesSet, joinType);
    }

    @Deprecated // to be removed before 2.0
    protected SubStraitJoin(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode left,
        RelNode right,
        RexNode condition,
        JoinRelType joinType,
        Set<String> variablesStopped)
        throws InvalidRelException {
      this(cluster, traitSet, left, right, condition,
          CorrelationId.setOf(variablesStopped), joinType);
    }

    @Override public SubStraitJoin copy(RelTraitSet traitSet, RexNode condition,
        RelNode left, RelNode right, JoinRelType joinType,
        boolean semiJoinDone) {
      try {
        return new SubStraitJoin(getCluster(), traitSet, left, right,
            condition, variablesSet, joinType);
      } catch (InvalidRelException e) {
        // Semantic error not possible. Must be a bug. Convert to
        // internal error.
        throw new AssertionError(e);
      }
    }

    @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      // We always "build" the
      double rowCount = mq.getRowCount(this);

      return planner.getCostFactory().makeCost(rowCount, 0, 0);
    }

    @Override public double estimateRowCount(RelMetadataQuery mq) {
      final double leftRowCount = left.estimateRowCount(mq);
      final double rightRowCount = right.estimateRowCount(mq);
      return Math.max(leftRowCount, rightRowCount);
    }

    @Override public SubStraitImplementor.Result implement(SubStraitImplementor implementor) {
      return implementor.implement(this);
    }
  }

  /** Calc operator implemented in SubStrait convention.
   *
   * @see org.apache.calcite.rel.core.Calc
   * */
  @Deprecated // to be removed before 2.0
  public static class SubStraitCalc extends SingleRel implements SubStraitRel {
    private final RexProgram program;

    public SubStraitCalc(RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        RexProgram program) {
      super(cluster, traitSet, input);
      assert getConvention() instanceof SubStraitConvention;
      this.program = program;
      this.rowType = program.getOutputRowType();
    }

    @Deprecated // to be removed before 2.0
    public SubStraitCalc(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
        RexProgram program, int flags) {
      this(cluster, traitSet, input, program);
      Util.discard(flags);
    }

    @Override public RelWriter explainTerms(RelWriter pw) {
      return program.explainCalc(super.explainTerms(pw));
    }

    @Override public double estimateRowCount(RelMetadataQuery mq) {
      return RelMdUtil.estimateFilteredRows(getInput(), program, mq);
    }

    @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      double dRows = mq.getRowCount(this);
      double dCpu = mq.getRowCount(getInput())
          * program.getExprCount();
      double dIo = 0;
      return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new SubStraitCalc(getCluster(), traitSet, sole(inputs), program);
    }

    @Override public SubStraitImplementor.Result implement(SubStraitImplementor implementor) {
      return implementor.implement(this);
    }
  }

  /**
   * Rule to convert a {@link Project} to
   * an {@link SubStraitProject}.
   */
  public static class SubStraitProjectRule extends SubStraitConverterRule {
    /** Creates a SubStraitProjectRule. */
    public static SubStraitProjectRule create(SubStraitConvention out) {
      return Config.INSTANCE
          .withConversion(Project.class, project -> false,
              Convention.NONE, out, "SubStraitProjectRule")
          .withRuleFactory(SubStraitProjectRule::new)
          .toRule(SubStraitProjectRule.class);
    }

    /** Called from the Config. */
    protected SubStraitProjectRule(Config config) {
      super(config);
    }

    private static boolean userDefinedFunctionInProject(Project project) {
      CheckingUserDefinedFunctionVisitor visitor = new CheckingUserDefinedFunctionVisitor();
      for (RexNode node : project.getProjects()) {
        node.accept(visitor);
        if (visitor.containsUserDefinedFunction()) {
          return true;
        }
      }
      return false;
    }

    @Override public boolean matches(RelOptRuleCall call) {
      Project project = call.rel(0);
      return project.getVariablesSet().isEmpty();
    }

    @Override public @Nullable RelNode convert(RelNode rel) {
      final Project project = (Project) rel;

      return new SubStraitProject(
          rel.getCluster(),
          rel.getTraitSet().replace(out),
          convert(
              project.getInput(),
              project.getInput().getTraitSet().replace(out)),
          project.getProjects(),
          project.getRowType());
    }
  }

  /** Implementation of {@link Project} in
   * {@link SubStraitConvention jdbc calling convention}. */
  public static class SubStraitProject
      extends Project
      implements SubStraitRel {
    public SubStraitProject(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        List<? extends RexNode> projects,
        RelDataType rowType) {
      super(cluster, traitSet, ImmutableList.of(), input, projects, rowType, ImmutableSet.of());
      assert getConvention() instanceof SubStraitConvention;
    }

    @Deprecated // to be removed before 2.0
    public SubStraitProject(RelOptCluster cluster, RelTraitSet traitSet,
        RelNode input, List<RexNode> projects, RelDataType rowType, int flags) {
      this(cluster, traitSet, input, projects, rowType);
      Util.discard(flags);
    }

    @Override public SubStraitProject copy(RelTraitSet traitSet, RelNode input,
        List<RexNode> projects, RelDataType rowType) {
      return new SubStraitProject(getCluster(), traitSet, input, projects, rowType);
    }

    @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      RelOptCost cost = super.computeSelfCost(planner, mq);
      if (cost == null) {
        return null;
      }
      return cost.multiplyBy(SubStraitConvention.COST_MULTIPLIER);
    }

    @Override public SubStraitImplementor.Result implement(SubStraitImplementor implementor) {
      return implementor.implement(this);
    }
  }

  /**
   * Rule to convert a {@link Filter} to
   * an {@link SubStraitFilter}.
   */
  public static class SubStraitFilterRule extends SubStraitConverterRule {
    /** Creates a SubStraitFilterRule. */
    public static SubStraitFilterRule create(SubStraitConvention out) {
      return Config.INSTANCE
          .withConversion(Filter.class, r -> !userDefinedFunctionInFilter(r),
              Convention.NONE, out, "SubStraitFilterRule")
          .withRuleFactory(SubStraitFilterRule::new)
          .toRule(SubStraitFilterRule.class);
    }

    /** Called from the Config. */
    protected SubStraitFilterRule(Config config) {
      super(config);
    }

    private static boolean userDefinedFunctionInFilter(Filter filter) {
      CheckingUserDefinedFunctionVisitor visitor = new CheckingUserDefinedFunctionVisitor();
      filter.getCondition().accept(visitor);
      return visitor.containsUserDefinedFunction();
    }

    @Override public @Nullable RelNode convert(RelNode rel) {
      final Filter filter = (Filter) rel;

      return new SubStraitFilter(
          rel.getCluster(),
          rel.getTraitSet().replace(out),
          convert(filter.getInput(),
              filter.getInput().getTraitSet().replace(out)),
          filter.getCondition());
    }
  }

  /** Implementation of {@link Filter} in
   * {@link SubStraitConvention jdbc calling convention}. */
  public static class SubStraitFilter extends Filter implements SubStraitRel {
    public SubStraitFilter(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        RexNode condition) {
      super(cluster, traitSet, input, condition);
      assert getConvention() instanceof SubStraitConvention;
    }

    @Override public SubStraitFilter copy(RelTraitSet traitSet, RelNode input,
        RexNode condition) {
      return new SubStraitFilter(getCluster(), traitSet, input, condition);
    }

    @Override public SubStraitImplementor.Result implement(SubStraitImplementor implementor) {
      return implementor.implement(this);
    }
  }

  /**
   * Rule to convert a {@link Aggregate}
   * to a {@link SubStraitAggregate}.
   */
  public static class SubStraitAggregateRule extends SubStraitConverterRule {
    /** Creates a SubStraitAggregateRule. */
    public static SubStraitAggregateRule create(SubStraitConvention out) {
      return Config.INSTANCE
          .withConversion(Aggregate.class, Convention.NONE, out,
              "SubStraitAggregateRule")
          .withRuleFactory(SubStraitAggregateRule::new)
          .toRule(SubStraitAggregateRule.class);
    }

    /** Called from the Config. */
    protected SubStraitAggregateRule(Config config) {
      super(config);
    }

    @Override public @Nullable RelNode convert(RelNode rel) {
      final Aggregate agg = (Aggregate) rel;
      if (agg.getGroupSets().size() != 1) {
        // GROUPING SETS not supported; see
        // [CALCITE-734] Push GROUPING SETS to underlying SQL via SubStrait adapter
        return null;
      }
      final RelTraitSet traitSet =
          agg.getTraitSet().replace(out);
      try {
        return new SubStraitAggregate(rel.getCluster(), traitSet,
            convert(agg.getInput(), out), agg.getGroupSet(),
            agg.getGroupSets(), agg.getAggCallList());
      } catch (InvalidRelException e) {
        LOGGER.debug(e.toString());
        return null;
      }
    }
  }

  /** Returns whether this SubStrait data source can implement a given aggregate
   * function. */
  private static boolean canImplement(AggregateCall aggregateCall,
      SqlDialect sqlDialect) {
    return sqlDialect.supportsAggregateFunction(
        aggregateCall.getAggregation().getKind())
        && aggregateCall.distinctKeys == null;
  }

  /** Aggregate operator implemented in SubStrait convention. */
  public static class SubStraitAggregate extends Aggregate implements SubStraitRel {
    public SubStraitAggregate(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        ImmutableBitSet groupSet,
        @Nullable List<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls)
        throws InvalidRelException {
      super(cluster, traitSet, ImmutableList.of(), input, groupSet, groupSets, aggCalls);
      assert getConvention() instanceof SubStraitConvention;
      assert this.groupSets.size() == 1 : "Grouping sets not supported";
      final SqlDialect dialect = null;
      for (AggregateCall aggCall : aggCalls) {
        if (!canImplement(aggCall, dialect)) {
          throw new InvalidRelException("cannot implement aggregate function "
              + aggCall);
        }
        if (aggCall.hasFilter() && !dialect.supportsAggregateFunctionFilter()) {
          throw new InvalidRelException("dialect does not support aggregate "
              + "functions FILTER clauses");
        }
      }
    }

    @Deprecated // to be removed before 2.0
    public SubStraitAggregate(RelOptCluster cluster, RelTraitSet traitSet,
        RelNode input, boolean indicator, ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls)
        throws InvalidRelException {
      this(cluster, traitSet, input, groupSet, groupSets, aggCalls);
      checkIndicator(indicator);
    }

    @Override public SubStraitAggregate copy(RelTraitSet traitSet, RelNode input,
        ImmutableBitSet groupSet,
        @Nullable List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
      try {
        return new SubStraitAggregate(getCluster(), traitSet, input,
            groupSet, groupSets, aggCalls);
      } catch (InvalidRelException e) {
        // Semantic error not possible. Must be a bug. Convert to
        // internal error.
        throw new AssertionError(e);
      }
    }

    @Override public SubStraitImplementor.Result implement(SubStraitImplementor implementor) {
      return implementor.implement(this);
    }
  }

  /**
   * Rule to convert a {@link Sort} to an
   * {@link SubStraitSort}.
   */
  public static class SubStraitSortRule extends SubStraitConverterRule {
    /** Creates a SubStraitSortRule. */
    public static SubStraitSortRule create(SubStraitConvention out) {
      return Config.INSTANCE
          .withConversion(Sort.class, Convention.NONE, out, "SubStraitSortRule")
          .withRuleFactory(SubStraitSortRule::new)
          .toRule(SubStraitSortRule.class);
    }

    /** Called from the Config. */
    protected SubStraitSortRule(Config config) {
      super(config);
    }

    @Override public @Nullable RelNode convert(RelNode rel) {
      return convert((Sort) rel, true);
    }

    /**
     * Converts a {@code Sort} into a {@code SubStraitSort}.
     *
     * @param sort Sort operator to convert
     * @param convertInputTraits Whether to convert input to {@code sort}'s
     *                            SubStrait convention
     * @return A new SubStraitSort
     */
    public RelNode convert(Sort sort, boolean convertInputTraits) {
      final RelTraitSet traitSet = sort.getTraitSet().replace(out);

      final RelNode input;
      if (convertInputTraits) {
        final RelTraitSet inputTraitSet = sort.getInput().getTraitSet().replace(out);
        input = convert(sort.getInput(), inputTraitSet);
      } else {
        input = sort.getInput();
      }

      return new SubStraitSort(sort.getCluster(), traitSet,
          input, sort.getCollation(), sort.offset, sort.fetch);
    }
  }

  /** Sort operator implemented in SubStrait convention. */
  public static class SubStraitSort
      extends Sort
      implements SubStraitRel {
    public SubStraitSort(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        RelCollation collation,
        @Nullable RexNode offset,
        @Nullable RexNode fetch) {
      super(cluster, traitSet, input, collation, offset, fetch);
      assert getConvention() instanceof SubStraitConvention;
      assert getConvention() == input.getConvention();
    }

    @Override public SubStraitSort copy(RelTraitSet traitSet, RelNode newInput,
        RelCollation newCollation, @Nullable RexNode offset, @Nullable RexNode fetch) {
      return new SubStraitSort(getCluster(), traitSet, newInput, newCollation,
          offset, fetch);
    }

    @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      RelOptCost cost = super.computeSelfCost(planner, mq);
      if (cost == null) {
        return null;
      }
      return cost.multiplyBy(0.9);
    }

    @Override public SubStraitImplementor.Result implement(SubStraitImplementor implementor) {
      return implementor.implement(this);
    }
  }

  /**
   * Rule to convert an {@link Union} to a
   * {@link SubStraitUnion}.
   */
  public static class SubStraitUnionRule extends SubStraitConverterRule {
    /** Creates a SubStraitUnionRule. */
    public static SubStraitUnionRule create(SubStraitConvention out) {
      return Config.INSTANCE
          .withConversion(Union.class, Convention.NONE, out, "SubStraitUnionRule")
          .withRuleFactory(SubStraitUnionRule::new)
          .toRule(SubStraitUnionRule.class);
    }

    /** Called from the Config. */
    protected SubStraitUnionRule(Config config) {
      super(config);
    }

    @Override public @Nullable RelNode convert(RelNode rel) {
      final Union union = (Union) rel;
      final RelTraitSet traitSet =
          union.getTraitSet().replace(out);
      return new SubStraitUnion(rel.getCluster(), traitSet,
          convertList(union.getInputs(), out), union.all);
    }
  }

  /** Union operator implemented in SubStrait convention. */
  public static class SubStraitUnion extends Union implements SubStraitRel {
    public SubStraitUnion(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelNode> inputs,
        boolean all) {
      super(cluster, traitSet, inputs, all);
    }

    @Override public SubStraitUnion copy(
        RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
      return new SubStraitUnion(getCluster(), traitSet, inputs, all);
    }

    @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      RelOptCost cost = super.computeSelfCost(planner, mq);
      if (cost == null) {
        return null;
      }
      return cost.multiplyBy(.1);
    }

    @Override public SubStraitImplementor.Result implement(SubStraitImplementor implementor) {
      return implementor.implement(this);
    }
  }

  /**
   * Rule to convert a {@link Intersect}
   * to a {@link SubStraitIntersect}.
   */
  public static class SubStraitIntersectRule extends SubStraitConverterRule {
    /** Creates a SubStraitIntersectRule. */
    public static SubStraitIntersectRule create(SubStraitConvention out) {
      return Config.INSTANCE
          .withConversion(Intersect.class, Convention.NONE, out,
              "SubStraitIntersectRule")
          .withRuleFactory(SubStraitIntersectRule::new)
          .toRule(SubStraitIntersectRule.class);
    }

    /** Called from the Config. */
    protected SubStraitIntersectRule(Config config) {
      super(config);
    }

    @Override public @Nullable RelNode convert(RelNode rel) {
      final Intersect intersect = (Intersect) rel;
      if (intersect.all) {
        return null; // INTERSECT ALL not implemented
      }
      final RelTraitSet traitSet =
          intersect.getTraitSet().replace(out);
      return new SubStraitIntersect(rel.getCluster(), traitSet,
          convertList(intersect.getInputs(), out), false);
    }
  }

  /** Intersect operator implemented in SubStrait convention. */
  public static class SubStraitIntersect
      extends Intersect
      implements SubStraitRel {
    public SubStraitIntersect(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelNode> inputs,
        boolean all) {
      super(cluster, traitSet, inputs, all);
      assert !all;
    }

    @Override public SubStraitIntersect copy(
        RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
      return new SubStraitIntersect(getCluster(), traitSet, inputs, all);
    }

    @Override public SubStraitImplementor.Result implement(SubStraitImplementor implementor) {
      return implementor.implement(this);
    }
  }

  /**
   * Rule to convert a {@link Minus} to a
   * {@link SubStraitMinus}.
   */
  public static class SubStraitMinusRule extends SubStraitConverterRule {
    /** Creates a SubStraitMinusRule. */
    public static SubStraitMinusRule create(SubStraitConvention out) {
      return Config.INSTANCE
          .withConversion(Minus.class, Convention.NONE, out, "SubStraitMinusRule")
          .withRuleFactory(SubStraitMinusRule::new)
          .toRule(SubStraitMinusRule.class);
    }

    /** Called from the Config. */
    protected SubStraitMinusRule(Config config) {
      super(config);
    }

    @Override public @Nullable RelNode convert(RelNode rel) {
      final Minus minus = (Minus) rel;
      if (minus.all) {
        return null; // EXCEPT ALL not implemented
      }
      final RelTraitSet traitSet =
          rel.getTraitSet().replace(out);
      return new SubStraitMinus(rel.getCluster(), traitSet,
          convertList(minus.getInputs(), out), false);
    }
  }

  /** Minus operator implemented in SubStrait convention. */
  public static class SubStraitMinus extends Minus implements SubStraitRel {
    public SubStraitMinus(RelOptCluster cluster, RelTraitSet traitSet,
        List<RelNode> inputs, boolean all) {
      super(cluster, traitSet, inputs, all);
      assert !all;
    }

    @Override public SubStraitMinus copy(RelTraitSet traitSet, List<RelNode> inputs,
        boolean all) {
      return new SubStraitMinus(getCluster(), traitSet, inputs, all);
    }

    @Override public SubStraitImplementor.Result implement(SubStraitImplementor implementor) {
      return implementor.implement(this);
    }
  }

  /** Rule that converts a table-modification to SubStrait. */
  public static class SubStraitTableModificationRule extends SubStraitConverterRule {
    /** Creates a SubStraitToEnumerableConverterRule. */
    public static SubStraitTableModificationRule create(SubStraitConvention out) {
      return Config.INSTANCE
          .withConversion(TableModify.class, Convention.NONE, out,
              "SubStraitTableModificationRule")
          .withRuleFactory(SubStraitTableModificationRule::new)
          .toRule(SubStraitTableModificationRule.class);
    }

    /** Called from the Config. */
    protected SubStraitTableModificationRule(Config config) {
      super(config);
    }

    @Override public @Nullable RelNode convert(RelNode rel) {
      final TableModify modify =
          (TableModify) rel;
      final ModifiableTable modifiableTable =
          modify.getTable().unwrap(ModifiableTable.class);
      if (modifiableTable == null) {
        return null;
      }
      final RelTraitSet traitSet =
          modify.getTraitSet().replace(out);
      return new SubStraitTableModify(
          modify.getCluster(), traitSet,
          modify.getTable(),
          modify.getCatalogReader(),
          convert(modify.getInput(), traitSet),
          modify.getOperation(),
          modify.getUpdateColumnList(),
          modify.getSourceExpressionList(),
          modify.isFlattened());
    }
  }

  /** Table-modification operator implemented in SubStrait convention. */
  public static class SubStraitTableModify extends TableModify implements SubStraitRel {
    public SubStraitTableModify(RelOptCluster cluster,
        RelTraitSet traitSet,
        RelOptTable table,
        Prepare.CatalogReader catalogReader,
        RelNode input,
        Operation operation,
        @Nullable List<String> updateColumnList,
        @Nullable List<RexNode> sourceExpressionList,
        boolean flattened) {
      super(cluster, traitSet, table, catalogReader, input, operation,
          updateColumnList, sourceExpressionList, flattened);
      assert input.getConvention() instanceof SubStraitConvention;
      assert getConvention() instanceof SubStraitConvention;
      final ModifiableTable modifiableTable =
          table.unwrap(ModifiableTable.class);
      if (modifiableTable == null) {
        throw new AssertionError(); // TODO: user error in validator
      }
      Expression expression = table.getExpression(Queryable.class);
      if (expression == null) {
        throw new AssertionError(); // TODO: user error in validator
      }
    }

    @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      RelOptCost cost = super.computeSelfCost(planner, mq);
      if (cost == null) {
        return null;
      }
      return cost.multiplyBy(.1);
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new SubStraitTableModify(
          getCluster(), traitSet, getTable(), getCatalogReader(),
          sole(inputs), getOperation(), getUpdateColumnList(),
          getSourceExpressionList(), isFlattened());
    }

    @Override public SubStraitImplementor.Result implement(SubStraitImplementor implementor) {
      return implementor.implement(this);
    }
  }

  /** Rule that converts a values operator to SubStrait. */
  public static class SubStraitValuesRule extends SubStraitConverterRule {
    /** Creates a SubStraitValuesRule. */
    public static SubStraitValuesRule create(SubStraitConvention out) {
      return Config.INSTANCE
          .withConversion(Values.class, Convention.NONE, out, "SubStraitValuesRule")
          .withRuleFactory(SubStraitValuesRule::new)
          .toRule(SubStraitValuesRule.class);
    }

    /** Called from the Config. */
    protected SubStraitValuesRule(Config config) {
      super(config);
    }

    @Override public @Nullable RelNode convert(RelNode rel) {
      Values values = (Values) rel;
      return new SubStraitValues(values.getCluster(), values.getRowType(),
          values.getTuples(), values.getTraitSet().replace(out));
    }
  }

  /** Values operator implemented in SubStrait convention. */
  public static class SubStraitValues extends Values implements SubStraitRel {
    SubStraitValues(RelOptCluster cluster, RelDataType rowType,
        ImmutableList<ImmutableList<RexLiteral>> tuples, RelTraitSet traitSet) {
      super(cluster, rowType, tuples, traitSet);
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      assert inputs.isEmpty();
      return new SubStraitValues(getCluster(), getRowType(), tuples, traitSet);
    }

    @Override public SubStraitImplementor.Result implement(SubStraitImplementor implementor) {
      return implementor.implement(this);
    }
  }

  /** Visitor that checks whether part of a projection is a user-defined
   * function (UDF). */
  private static class CheckingUserDefinedFunctionVisitor
      extends RexVisitorImpl<Void> {

    private boolean containsUsedDefinedFunction = false;

    CheckingUserDefinedFunctionVisitor() {
      super(true);
    }

    public boolean containsUserDefinedFunction() {
      return containsUsedDefinedFunction;
    }

    @Override public Void visitCall(RexCall call) {
      SqlOperator operator = call.getOperator();
      if (operator instanceof SqlFunction
          && ((SqlFunction) operator).getFunctionType().isUserDefined()) {
        containsUsedDefinedFunction |= true;
      }
      return super.visitCall(call);
    }

  }

}
