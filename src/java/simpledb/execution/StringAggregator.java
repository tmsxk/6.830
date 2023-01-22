package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int groupByField;
    private Type groupByFieldType;
    private int aggregateField;
    private Op what;
    private Map<String, List<String>> groupFieldMap;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.groupByField = gbfield;
        this.groupByFieldType = gbfieldtype;
        this.aggregateField = afield;
        this.what = what;
        this.groupFieldMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (groupByField == Aggregator.NO_GROUPING) {
            String aggregateValue = ((StringField) tup.getField(aggregateField)).getValue();
            if (!groupFieldMap.containsKey("")) {
                groupFieldMap.put("", new ArrayList<>());
            }
            groupFieldMap.get("").add(aggregateValue);
        } else {
            String groupByKey = tup.getField(groupByField).toString();
            String aggregateValue = ((StringField) tup.getField(aggregateField)).getValue();
            if (!groupFieldMap.containsKey(groupByKey)) {
                groupFieldMap.put(groupByKey, new ArrayList<>());
            }
            groupFieldMap.get(groupByKey).add(aggregateValue);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new StringAggregatorOpIterator();
    }

    private class StringAggregatorOpIterator implements OpIterator {

        private List<Tuple> res;
        private Iterator<Tuple> it;

        public StringAggregatorOpIterator() {
            res = new ArrayList<>();
            if (groupByField == Aggregator.NO_GROUPING) {
                Tuple t = new Tuple(getTupleDesc());
                Field aggregateValue = new IntField(calculate(groupFieldMap.get("")));
                t.setField(0, aggregateValue);
                res.add(t);
            } else {
                for (Map.Entry<String, List<String>> entry: groupFieldMap.entrySet()) {
                    Tuple t = new Tuple(getTupleDesc());
                    Field groupValue = null;
                    if (groupByFieldType == Type.INT_TYPE) {
                        groupValue = new IntField(Integer.parseInt(entry.getKey()));
                    } else {
                        groupValue = new StringField(entry.getKey(), entry.getKey().length());
                    }
                    Field aggregateValue = new IntField(calculate(entry.getValue()));
                    t.setField(0, groupValue);
                    t.setField(1, aggregateValue);
                    res.add(t);
                }
            }
        }

        private int calculate(List<String> values) {
            int res = 0;
            switch (what) {
                case COUNT:
                    res = (int) values.stream().count();
                    break;
                default:
                    throw new UnsupportedOperationException("unsupported aggregate operator");
            }
            return res;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            it = res.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (it == null) {
                throw new IllegalStateException("IntegerAggregator not open");
            }
            return it.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (it == null) {
                throw new IllegalStateException("IntegerAggregator not open");
            }
            return it.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            if (it == null) {
                throw new IllegalStateException("IntegerAggregator not open");
            }
            it = res.iterator();
        }

        @Override
        public TupleDesc getTupleDesc() {
            if (groupByField == Aggregator.NO_GROUPING) {
                return new TupleDesc(new Type[]{Type.INT_TYPE});
            } else {
                return new TupleDesc(new Type[]{groupByFieldType, Type.INT_TYPE});
            }
        }

        @Override
        public void close() {
            it = null;
        }
    }

}
