// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.common.proc;

import org.apache.doris.alter.AlterJobV2;
import org.apache.doris.alter.MaterializedViewHandler;
import org.apache.doris.alter.RollupJobV2;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.LimitElement;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.common.util.OrderByPair;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.coercion.DateLikeType;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class RollupProcDir implements ProcDirInterface {
    private static final Logger LOG = LogManager.getLogger(RollupProcDir.class);

    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("JobId").add("TableName").add("CreateTime").add("FinishTime")
            .add("BaseIndexName").add("RollupIndexName").add("RollupId").add("TransactionId")
            .add("State").add("Msg").add("Progress").add("Timeout")
            .build();

    private MaterializedViewHandler materializedViewHandler;
    private Database db;

    public RollupProcDir(MaterializedViewHandler materializedViewHandler, Database db) {
        this.materializedViewHandler = materializedViewHandler;
        this.db = db;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(materializedViewHandler);

        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        List<List<Comparable>> rollupJobInfos;
        // db is null means need total result of all databases
        if (db == null) {
            rollupJobInfos = materializedViewHandler.getAllAlterJobInfos();
        } else {
            rollupJobInfos = materializedViewHandler.getAlterJobInfosByDb(db);
        }
        for (List<Comparable> infoStr : rollupJobInfos) {
            List<String> oneInfo = new ArrayList<String>(TITLE_NAMES.size());
            for (Comparable element : infoStr) {
                oneInfo.add(element.toString());
            }
            result.addRow(oneInfo);
        }
        return result;
    }

    public ProcResult fetchResultByFilter(HashMap<String, Expr> filter, ArrayList<OrderByPair> orderByPairs,
                                          LimitElement limitElement) throws AnalysisException {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(materializedViewHandler);

        List<List<Comparable>> rollupJobInfos = materializedViewHandler.getAlterJobInfosByDb(db);
        List<List<Comparable>> jobInfos = Lists.newArrayList();

        //where
        if (filter == null || filter.size() == 0) {
            jobInfos = rollupJobInfos;
        } else {
            jobInfos = Lists.newArrayList();
            for (List<Comparable> infoStr : rollupJobInfos) {
                if (infoStr.size() != TITLE_NAMES.size()) {
                    LOG.warn("RollupJobInfos.size() " + rollupJobInfos.size()
                            + " not equal TITLE_NAMES.size() " + TITLE_NAMES.size());
                    continue;
                }
                boolean isNeed = true;
                for (int i = 0; i < infoStr.size(); i++) {
                    isNeed = filterResult(TITLE_NAMES.get(i), infoStr.get(i), filter);
                    if (!isNeed) {
                        break;
                    }
                }
                if (isNeed) {
                    jobInfos.add(infoStr);
                }
            }
        }

        // order by
        if (orderByPairs != null) {
            ListComparator<List<Comparable>> comparator = null;
            OrderByPair[] orderByPairArr = new OrderByPair[orderByPairs.size()];
            comparator = new ListComparator<List<Comparable>>(orderByPairs.toArray(orderByPairArr));
            Collections.sort(jobInfos, comparator);
        }

        //limit
        if (limitElement != null && limitElement.hasLimit()) {
            int beginIndex = (int) limitElement.getOffset();
            int endIndex = (int) (beginIndex + limitElement.getLimit());
            if (endIndex > jobInfos.size()) {
                endIndex = jobInfos.size();
            }
            jobInfos = jobInfos.subList(beginIndex, endIndex);
        }

        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        for (List<Comparable> jobInfo : jobInfos) {
            List<String> oneResult = new ArrayList<String>(jobInfos.size());
            for (Comparable column : jobInfo) {
                oneResult.add(column.toString());
            }
            result.addRow(oneResult);
        }
        return result;
    }

    public ProcResult fetchResultByFilterExpression(HashMap<String, Expression> filter,
                                                        ArrayList<OrderByPair> orderByPairs,
                                                        LimitElement limitElement) throws AnalysisException {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(materializedViewHandler);

        List<List<Comparable>> rollupJobInfos = materializedViewHandler.getAlterJobInfosByDb(db);
        List<List<Comparable>> jobInfos = Lists.newArrayList();

        //where
        if (filter == null || filter.size() == 0) {
            jobInfos = rollupJobInfos;
        } else {
            jobInfos = Lists.newArrayList();

            // if filter's keys do not contain any title, we should do nothing
            Set<String> lowercaseKeys = filter.keySet().stream()
                    .map(String::toLowerCase)
                    .collect(Collectors.toSet());
            boolean containsAnyTitle = TITLE_NAMES.stream()
                    .map(String::toLowerCase)
                    .anyMatch(lowercaseKeys::contains);
            if (containsAnyTitle) {
                for (List<Comparable> infoStr : rollupJobInfos) {
                    if (infoStr.size() != TITLE_NAMES.size()) {
                        LOG.warn("RollupJobInfos.size() " + rollupJobInfos.size()
                                + " not equal TITLE_NAMES.size() " + TITLE_NAMES.size());
                        continue;
                    }
                    boolean isNeed = true;
                    for (int i = 0; i < infoStr.size(); i++) {
                        isNeed = filterResultExpression(TITLE_NAMES.get(i), infoStr.get(i), filter);
                        if (!isNeed) {
                            break;
                        }
                    }
                    if (isNeed) {
                        jobInfos.add(infoStr);
                    }
                }
            }
        }

        // order by
        if (orderByPairs != null) {
            ListComparator<List<Comparable>> comparator = null;
            OrderByPair[] orderByPairArr = new OrderByPair[orderByPairs.size()];
            comparator = new ListComparator<List<Comparable>>(orderByPairs.toArray(orderByPairArr));
            Collections.sort(jobInfos, comparator);
        }

        //limit
        if (limitElement != null && limitElement.hasLimit()) {
            int beginIndex = (int) limitElement.getOffset();
            int endIndex = (int) (beginIndex + limitElement.getLimit());
            if (endIndex > jobInfos.size()) {
                endIndex = jobInfos.size();
            }
            if (beginIndex > endIndex) {
                beginIndex = endIndex;
            }
            jobInfos = jobInfos.subList(beginIndex, endIndex);
        }

        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        for (List<Comparable> jobInfo : jobInfos) {
            List<String> oneResult = new ArrayList<String>(jobInfos.size());
            for (Comparable column : jobInfo) {
                oneResult.add(column.toString());
            }
            result.addRow(oneResult);
        }
        return result;
    }

    boolean filterResultExpression(String columnName, Comparable element,
                                   HashMap<String, Expression> filter) throws AnalysisException {
        if (filter == null) {
            return true;
        }
        Expression subExpr = filter.get(columnName.toLowerCase());
        if (subExpr == null) {
            return true;
        }

        if (subExpr instanceof ComparisonPredicate) {
            return filterSubExpression(subExpr, element);
        } else if (subExpr instanceof Not) {
            subExpr = subExpr.child(0);
            if (subExpr instanceof EqualTo) {
                return !filterSubExpression(subExpr, element);
            }
        }

        return false;
    }

    boolean filterSubExpression(Expression expr, Comparable element) throws AnalysisException {
        if (expr instanceof EqualTo && expr.child(1) instanceof StringLikeLiteral) {
            return ((StringLikeLiteral) expr.child(1)).getValue().equals(element);
        }

        long leftVal;
        long rightVal;

        if (expr.child(1) instanceof org.apache.doris.nereids.trees.expressions.literal.DateLiteral) {
            DateLikeType dateLikeType;
            if (expr.child(1) instanceof DateV2Literal) {
                leftVal = (new org.apache.doris.nereids.trees.expressions.literal.DateV2Literal(
                    (String) element)).getValue();
            } else if (expr.child(1) instanceof DateTimeLiteral) {
                leftVal = (new org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral(
                    (String) element)).getValue();
            } else if (expr.child(1) instanceof DateTimeV2Literal) {
                dateLikeType = DateTimeV2Type.MAX;
                leftVal = (new org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal(
                    (DateTimeV2Type) dateLikeType, (String) element)).getValue();
            } else {
                throw new AnalysisException("Invalid date type: " + expr.child(1).getDataType());
            }
            rightVal = ((org.apache.doris.nereids.trees.expressions.literal.DateLiteral) expr.child(1)).getValue();
            if (expr instanceof EqualTo) {
                return leftVal == rightVal;
            } else if (expr instanceof NullSafeEqual) {
                return leftVal == rightVal;
            } else if (expr instanceof GreaterThan) {
                return leftVal > rightVal;
            } else if (expr instanceof GreaterThanEqual) {
                return leftVal >= rightVal;
            } else if (expr instanceof LessThan) {
                return leftVal < rightVal;
            } else if (expr instanceof LessThanEqual) {
                return leftVal <= rightVal;
            } else {
                Preconditions.checkState(false, "No defined binary operator.");
            }
        }
        return true;
    }

    boolean filterResult(String columnName, Comparable element, HashMap<String, Expr> filter) throws AnalysisException {
        if (filter == null) {
            return true;
        }
        Expr subExpr = filter.get(columnName.toLowerCase());
        if (subExpr == null) {
            return true;
        }
        BinaryPredicate binaryPredicate = (BinaryPredicate) subExpr;
        if (subExpr.getChild(1) instanceof StringLiteral && binaryPredicate.getOp() == BinaryPredicate.Operator.EQ) {
            return ((StringLiteral) subExpr.getChild(1)).getValue().equals(element);
        }
        if (subExpr.getChild(1) instanceof DateLiteral) {
            Type type;
            switch (subExpr.getChild(1).getType().getPrimitiveType()) {
                case DATE:
                case DATETIME:
                    type = Type.DATETIME;
                    break;
                case DATEV2:
                    type = Type.DATETIMEV2;
                    break;
                case DATETIMEV2:
                    type = subExpr.getChild(1).getType();
                    break;
                default:
                    throw new AnalysisException("Invalid date type: " + subExpr.getChild(1).getType());
            }
            Long leftVal = (new DateLiteral((String) element, type)).getLongValue();
            Long rightVal = ((DateLiteral) subExpr.getChild(1)).getLongValue();
            switch (binaryPredicate.getOp()) {
                case EQ:
                case EQ_FOR_NULL:
                    return leftVal.equals(rightVal);
                case GE:
                    return leftVal >= rightVal;
                case GT:
                    return leftVal > rightVal;
                case LE:
                    return leftVal <= rightVal;
                case LT:
                    return leftVal < rightVal;
                case NE:
                    return !leftVal.equals(rightVal);
                default:
                    Preconditions.checkState(false, "No defined binary operator.");
            }
        }
        return true;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String jobIdStr) throws AnalysisException {
        if (Strings.isNullOrEmpty(jobIdStr)) {
            throw new AnalysisException("Job id is null");
        }

        long jobId = -1L;
        try {
            jobId = Long.valueOf(jobIdStr);
        } catch (Exception e) {
            throw new AnalysisException("Job id is invalid");
        }

        Preconditions.checkState(jobId != -1L);
        AlterJobV2 job = materializedViewHandler.getUnfinishedAlterJobV2ByJobId(jobId);
        if (job == null) {
            return null;
        }

        return new RollupJobProcNode((RollupJobV2) job);
    }

}
