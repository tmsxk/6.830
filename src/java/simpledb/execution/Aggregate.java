package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private int groupByField;
    private int aggregateField;
    Aggregator.Op aop;
    OpIterator[] children;
    Aggregator aggregator;
    OpIterator it;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.groupByField = gfield;
        this.aggregateField = afield;
        this.aop = aop;
        this.children = new OpIterator[1];
        children[0] = child;
        it = null;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        return groupByField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        if (groupByField != Aggregator.NO_GROUPING) {
            return children[0].getTupleDesc().getFieldName(groupByField);
        }
        return null;
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return aggregateField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        return children[0].getTupleDesc().getFieldName(aggregateField);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        children[0].open();
        Type groupByFieldType = groupByField == Aggregator.NO_GROUPING? null:
                children[0].getTupleDesc().getFieldType(groupByField);
        Type aggregateFieldType = children[0].getTupleDesc().getFieldType(aggregateField);
        if (aggregateFieldType == Type.INT_TYPE) {
            this.aggregator = new IntegerAggregator(groupByField, groupByFieldType, aggregateField, aop);
        } else {
            this.aggregator = new StringAggregator(groupByField, groupByFieldType, aggregateField, aop);
        }
        while (children[0].hasNext()) {
            aggregator.mergeTupleIntoGroup(children[0].next());
        }
        children[0].close();
        super.open();
        it = aggregator.iterator();
        it.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (it.hasNext()) {
            return it.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        close();
        open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        if (groupByField == Aggregator.NO_GROUPING) {
            return new TupleDesc(new Type[]{Type.INT_TYPE},
                    new String[]{aop.toString()+"("+children[0].getTupleDesc().getFieldName(aggregateField)+")"});
        } else {
            return new TupleDesc(new Type[]{children[0].getTupleDesc().getFieldType(groupByField), Type.INT_TYPE},
                    new String[]{children[0].getTupleDesc().getFieldName(groupByField),
                            aop.toString()+"("+children[0].getTupleDesc().getFieldName(aggregateField)+")"});
        }
    }

    public void close() {
        // some code goes here
        super.close();
        it.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.children = new OpIterator[children.length];
        for (int i = 0; i < children.length; i++) {
            this.children[i] = children[i];
        }
    }

}
