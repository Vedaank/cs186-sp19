package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;

public class PNLJOperator extends JoinOperator {
    public PNLJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        Database.Transaction transaction) throws QueryPlanException, DatabaseException {
        super(leftSource,
              rightSource,
              leftColumnName,
              rightColumnName,
              transaction,
              JoinType.PNLJ);

        // for HW4
        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
        return new PNLJIterator();
    }

    public int estimateIOCost() throws QueryPlanException {
        //does nothing
        return 0;
    }

    /**
     * PNLJ: Page Nested Loop Join
     *  See lecture slides.
     *
     * An implementation of Iterator that provides an iterator interface for this operator.
     *
     * Before proceeding, you should read and understand SNLJOperator.java
     *    You can find it in the same directory as this file.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given (Once again,
     *    SNLJOperator.java might prove to be a useful reference).
     */
    private class PNLJIterator extends JoinIterator {
        /**
         * Some member variables are provided for guidance, but there are many possible solutions.
         * You should implement the solution that's best for you, using any member variables you need.
         * You're free to use these member variables, but you're not obligated to.
         */

        private Iterator<Page> leftIterator = null;
        private Iterator<Page> rightIterator = null;
        private BacktrackingIterator<Record> leftRecordIterator = null;
        private BacktrackingIterator<Record> rightRecordIterator = null;
        private Record leftRecord = null;
        private Record nextRecord = null;

        public PNLJIterator() throws QueryPlanException, DatabaseException {
            super();
            // throw new UnsupportedOperationException("TODO(hw3): implement");
            this.leftIterator = PNLJOperator.this.getPageIterator(this.getLeftTableName());
            this.rightIterator = PNLJOperator.this.getPageIterator(this.getRightTableName());

            this.leftIterator.next();
            this.rightIterator.next();

            this.leftRecordIterator = PNLJOperator.this.getBlockIterator(getLeftTableName(), new Page[]{this.leftIterator.next()});
            this.rightRecordIterator = PNLJOperator.this.getBlockIterator(getRightTableName(), new Page[]{this.rightIterator.next()});

            this.leftRecord = leftRecordIterator.hasNext() ? leftRecordIterator.next() : null;
            this.nextRecord = null;

            if (leftRecord != null) {
                leftRecordIterator.mark();
                try {
                    fetchNextRecord();
                } catch (DatabaseException e) {
                    this.nextRecord = null;
                }
                if (this.rightRecordIterator.hasNext()) {
                    this.rightRecordIterator.mark();
                }
            }
            return;
        }

        private void resetRightRecord() {
            this.rightRecordIterator.reset();
            assert(rightRecordIterator.hasNext());
            rightRecordIterator.mark();
        }

        private void nextLeftRecord() throws DatabaseException {
            if (!this.leftIterator.hasNext() && !this.leftRecordIterator.hasNext()
                    && !this.rightIterator.hasNext() && !this.rightRecordIterator.hasNext()) {
                throw new DatabaseException("ERROR");
            }

            if (this.leftRecordIterator.hasNext()) {
                this.leftRecord = this.leftRecordIterator.next();
            } else {
                if (this.rightIterator.hasNext()) {
                    this.rightRecordIterator = PNLJOperator.this.getBlockIterator(getRightTableName(), new Page[]{this.rightIterator.next()});
                    this.leftRecordIterator.reset();
                    if (this.leftRecordIterator.hasNext()) {
                        this.leftRecord = this.leftRecordIterator.next();
                    } else {
                        this.leftRecordIterator = null;
                    }

                    if (this.rightRecordIterator.hasNext()) {
                        this.rightRecordIterator.next();
                        this.rightRecordIterator.mark();
                        this.rightRecordIterator.reset();
                    }
                    return;
                } else {
                    this.leftRecordIterator = PNLJOperator.this.getBlockIterator(getLeftTableName(), new Page[]{this.leftIterator.next()});
                    this.leftRecord = leftRecordIterator.hasNext() ? leftRecordIterator.next() : null;
                    
                    if (leftRecord != null) {
                        leftRecordIterator.mark();
                        this.rightIterator = PNLJOperator.this.getPageIterator(this.getRightTableName());
                        this.rightIterator.next();
                        this.rightRecordIterator = PNLJOperator.this.getBlockIterator(getRightTableName(), new Page[]{this.rightIterator.next()});
                        
                        if (this.rightRecordIterator.hasNext()) {
                            this.rightRecordIterator.next();
                            this.rightRecordIterator.mark();
                            this.rightRecordIterator.reset();
                        }
                    } 
                    return;
                }
            }
        }

        private void fetchNextRecord() throws DatabaseException {
            if (this.leftRecord == null) {
                throw new DatabaseException("No new page to fetch");
            }
            this.nextRecord = null;
            do {
                if (this.rightRecordIterator.hasNext()) {
                    Record rightRecord = rightRecordIterator.next();
                    DataBox leftJoinValue = this.leftRecord.getValues().get(PNLJOperator.this.getLeftColumnIndex());
                    DataBox rightJoinValue = rightRecord.getValues().get(PNLJOperator.this.getRightColumnIndex());
                    if (leftJoinValue.equals(rightJoinValue)) {
                        List<DataBox> leftValues = new ArrayList<>(this.leftRecord.getValues());
                        List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
                        leftValues.addAll(rightValues);
                        this.nextRecord = new Record(leftValues);
                    }
                } else {
                    nextLeftRecord();
                    resetRightRecord();
                }
            } while (!hasNext());
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        public boolean hasNext() {
            // throw new UnsupportedOperationException("TODO(hw3): implement");
            return this.nextRecord != null;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        public Record next() {
            // throw new UnsupportedOperationException("TODO(hw3): implement");
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            Record nextRecord = this.nextRecord;
            try {
                this.fetchNextRecord();
            } catch (DatabaseException e) {
                this.nextRecord = null;
            }
            return nextRecord;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}