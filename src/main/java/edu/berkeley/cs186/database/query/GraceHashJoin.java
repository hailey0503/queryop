package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.HashFunc;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.databox.*;
import edu.berkeley.cs186.database.memory.HashPartition;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import javax.xml.crypto.Data;
import java.util.*;
import java.util.function.Function;

public class GraceHashJoin {
    private Iterator<Record> leftRelationIterator;
    private Iterator<Record> rightRelationIterator;
    private int numBuffers;
    private int leftColumnIndex;
    private int rightColumnIndex;
    private TransactionContext transactionContext;
    private Schema leftSchema;
    private Schema rightSchema;

    // For the break methods actually materializing the records
    // may consume a lot of system resources
    private boolean materializeJoin;

    /**
     * Main constructor
     */
    public GraceHashJoin(Iterator<Record> leftRelationIterator, Iterator<Record> rightRelationIterator,
                         int leftColumnIndex, int rightColumnIndex,
                         TransactionContext transactionContext, Schema leftSchema, Schema rightSchema,
                         boolean materializeJoin) {
        this.leftRelationIterator = leftRelationIterator;
        this.rightRelationIterator = rightRelationIterator;
        this.numBuffers = transactionContext.getWorkMemSize();
        this.leftColumnIndex = leftColumnIndex;
        this.rightColumnIndex = rightColumnIndex;
        this.transactionContext = transactionContext;
        this.leftSchema = leftSchema;
        this.rightSchema = rightSchema;
        this.materializeJoin = materializeJoin;
    }

    /**
     * Partition stage.
     *
     * For every record in the given iterator, hashes the value from the
     * column we're joining on and adds it to the correct partition in
     * partitions.
     *
     * It will be helpful to take a look at HashPartition.java to see what methods you can use
     * to add the record to the correct partition.
     *
     */
    private void partition(HashPartition[] partitions, Iterator<Record> records, boolean left,
                           int pass) {
        assert pass >= 1;
        Function<DataBox, Integer> hashFunc = HashFunc.getHashFunction(pass); // Use this to hash!
        int columnIndex = left ? getLeftColumnIndex() : getRightColumnIndex();

        int usableBuffers = numBuffers - 1;
        while (records.hasNext()) { //correct iterator??
            Record rec = records.next();
            DataBox columnValue = rec.getValues().get(columnIndex);
            int hashcode = hashFunc.apply(columnValue);
            int partitionNum = (hashcode % partitions.length);
            if (partitionNum < 0) {
                partitionNum += partitions.length;    // Hash might be negative
            }
            //System.out.println(partitionNum);

            if (left) {
                partitions[partitionNum].addLeftRecord(rec);
            } else {
                partitions[partitionNum].addRightRecord(rec);
            }
        }

        // TODO(proj3_part1): implement the partitioning logic
        // You may find the implementation in NaiveHashJoin.java to be a good
        // starting point. Make sure to use the hash function we provide to you
        // by calling hashFunc.apply(databox) to get an integer valued hash code
        // from a DataBox object.
        // GraceHashJoin is that you use a different hash function on every single pass,
        // so we track which pass we're on using the pass variable

        // For left, try looking at how partition is called in run().
        // Also try looking at how partition is implemented in NaiveHashJoin.

        // recursive partitioning applications are handled in the for-loop of run.
        // This function doesn't return anything, it just hashes the value from the column we're joining on and adds it to the correct partition in partitions.
        // Your answer here should be fairly similar to the implementation in Naive Hash Join.
        //so am i on the right track to recursively call run in the for loop? --> YES
    }

    /**
     * Runs the buildAndProbe stage on a given partition.
     * To make things easier we set up 5 variables for you to use for the
     * building and probing phases.
     *
     * Returns a list of the joined records
     * @param partition An iterator of Records from a partition
     */
    private List<Record> buildAndProbe(HashPartition partition) {
        // Use these 5 variables in your implementation below!
        boolean probeFirst; // True if the probe records come from the left partition
        Iterator<Record> buildRecords; // We'll build our in memory hash table with these records
        Iterator<Record> probeRecords; // We'll probe the table with these records
        int buildColumnIndex; // The index of the join column for the build records
        int probeColumnIndex; // The index of the join column for the probe records

        if (partition.getNumLeftPages() <= this.numBuffers - 2) {
            buildRecords = partition.getLeftIterator();
            buildColumnIndex = getLeftColumnIndex();
            probeRecords = partition.getRightIterator();
            probeColumnIndex = getRightColumnIndex();
            probeFirst = false; // When we join the records, the record used to probe will be on the right
        } else if (partition.getNumRightPages() <= this.numBuffers - 2) {
            buildRecords = partition.getRightIterator();
            buildColumnIndex = getRightColumnIndex();
            probeRecords = partition.getLeftIterator();
            probeColumnIndex = getLeftColumnIndex();
            probeFirst = true; // When we join the records, the record used to probe will be on the left
        } else {
            throw new IllegalArgumentException(
                "Neither the left nor the right records in this partition " +
                "fit in B-2 pages of memory."
            );
        }
        if (!materializeJoin) {
            // For the student input tests we don't want to materialize the
            // joined records as it may consume excessive resources
            return new ArrayList<Record>();
        }

        // Add all the results of build and probe here
        ArrayList<Record> joinedRecords = new ArrayList<Record>();
        Map<DataBox, List<Record>> hashTable = new HashMap<>();

        // TODO(proj3_part1): implement the building and probing stage

        // You shouldn't refer to any variable starting with "left" or "right" here
        // Use the "build" and "probe" variables we set up for you
        // Check out how NaiveHashJoin implements this function if you feel stuck.

        //building stage
        //if (probeFirst) { //R
            while (buildRecords.hasNext()) {
                Record buildRecord = buildRecords.next();
                DataBox buildJoinValue = buildRecord.getValues().get(buildColumnIndex);
                if (!hashTable.containsKey(buildJoinValue)) {
                    hashTable.put(buildJoinValue, new ArrayList<>());
                }
                hashTable.get(buildJoinValue).add(buildRecord);
            }
        //probing stage
        while (probeRecords.hasNext()) {
            Record probeRecord = probeRecords.next();
            DataBox probeJoinValue = probeRecord.getValues().get(probeColumnIndex);
            if (hashTable.containsKey(probeJoinValue)) {
                for (Record bRecord : hashTable.get(probeJoinValue)) {
                    Record joinedRecord = joinRecords(bRecord, probeRecord, probeFirst);
                    joinedRecords.add(joinedRecord);
                }
            }
        }

        // Return the records
        return joinedRecords;
    }

    /**
     * Runs the grace hash join algorithm
     * Each pass starts by partitioning leftRecords and rightRecords.
     * If we can run build and probe on a partition we should immediately do so,
     * otherwise we should apply the grace hash join algorithm recursively to
     * break up the partitions further.
     *
     * Once all partitions have been conquered return a list of all the joined
     * records from leftRecords and rightRecords
     *
     * @return A list of joined records
     */
    private List<Record> run(Iterator<Record> leftRecords, Iterator<Record> rightRecords, int pass) {
        assert pass >= 1;

        if (pass > 5) {
            throw new IllegalStateException("Reached the max number of passes cap");
        }

        HashPartition[] partitions = createPartitions();
        this.partition(partitions, leftRecords, true, pass);
        this.partition(partitions, rightRecords, false, pass);

        // Accumulate all the records from the join here
        ArrayList<Record> joinedRecords = new ArrayList<>();
        int limit = numBuffers - 2;

        for (HashPartition partition : partitions) {
            // TODO(proj3_part1): implement the rest of grace hash join in this for loop
            //
            // If you can run the build and probe phase on a partition you should
            // do so immediately and add its records to joinedRecords.
            //
            // Otherwise you should add the results of recursively running
            // the grace hash join algorithm on the partition
            // to joinedRecords.
            //
            // You may find the ArrayList.addAll method useful here.

            // It may be helpful to read through the HashPartition.java class for methods that will be useful here
            // split an oversized partition to smaller sizes? ->  done in the skeleton code of run
            // Pass in the correct iterators in the recursive run call! It will save you an hour
            // i'm stuck on the iterator arguments. I'm unsure how to approach it but I know that we cant just pass in leftrecord/rightrecords as is.
            // Try looking around in the HashPartition code to see what methods are available to you
            if (partition.getNumLeftPages() <= limit || partition.getNumRightPages() <= limit) {
                joinedRecords.addAll(buildAndProbe(partition));

            } else {

                joinedRecords.addAll(run(partition.getLeftIterator(), partition.getRightIterator(), pass + 1));
            }
        }
        return joinedRecords;
    }

    public List<Record> begin() {
        // This starts the first pass. The rest is handled recursively by getJoinedRecordsHelper
        return run(this.leftRelationIterator, this.rightRelationIterator, 1);
    }

    /**
     * Create an appropriate number of HashPartitions relative to the
     * number of available buffers we have and return an array
     *
     * You should first know the answer to how many partitions will be created!
     *
     * @return an array of HashPartitions
     */
    private HashPartition[] createPartitions() {
        // TODO(proj3_part1): create an array of partitions
        // You may find the provided helper function
        // createPartition() useful here.
        int usableBuffers = this.numBuffers - 1;
        HashPartition partitions[] = new HashPartition[usableBuffers];
        for (int i = 0; i < usableBuffers; i++) {
            partitions[i] = createPartition();
        }
        return partitions;
    }

    // Feel free to add your own helper methods here if you wish to do so

    //////////////////////////////////////// BUILT-IN HELPER METHODS /////////////////////////////////////
    /**
     * Helper method to create a joined record from a record of the left relation
     * and a record of the right relation.
     * @param leftRecord Record from the left relation
     * @param rightRecord Record from the right relation
     * @return joined record
     */
    private Record joinRecords(Record leftRecord, Record rightRecord) {
        List<DataBox> leftValues = new ArrayList<>(leftRecord.getValues());
        List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
        leftValues.addAll(rightValues);
        return new Record(leftValues);
    }

    /**
     * Helper method to join together a record from the build stage to a record
     * from the probe stage. Since build or probe could be from either the left
     * or right partition, make sure you set probeFirst properly!
     * @param buildRecord record used in the build phase
     * @param probeRecord record used to probe
     * @param probeFirst true if the probe record is from the left relation, otherwise false
     * @return joined record
     */
    private Record joinRecords(Record buildRecord, Record probeRecord, boolean probeFirst) {
        if (probeFirst) {
            // Put the probe record first
            return joinRecords(probeRecord, buildRecord);
        }
        // Otherwise put the build record first
        return joinRecords(buildRecord, probeRecord);
    }

    /**
     * Creates a new hash partition
     * @return a new hash partition
     */
    private HashPartition createPartition() {
        return new HashPartition(transactionContext, leftSchema, rightSchema);
    }

    /**
     * Get left column index we are joining on
     * @return left column index
     */
    private int getLeftColumnIndex() {
        return this.leftColumnIndex;
    }

    /**
     * Get right column index we are joining on
     * @return right column index
     */
    private int getRightColumnIndex() {
        return this.rightColumnIndex;
    }

    /**
     * Creates a record using val as the value for a single column of type int
     * @param val value the field will take
     * @return a record
     */
    private static Record createRecord(int val) {
        List<DataBox> dataValues = new ArrayList<DataBox>();
        dataValues.add(new IntDataBox(val));
        return new Record(dataValues);
    }

    //////////////////////////////////////// STUDENT INPUT METHODS /////////////////////////////////////

    /**
     * This method is called in testBreakNHJButPassGHJ.
     *
     * Come up with two lists of records for leftRecords and rightRecords such that NHJ will error when given
     * those relations, but GHJ will successfully run.
     *
     * createRecord() takes in a value and returns a record with that value.
     *
     * Lists cannot be empty or the joins will error!
     *
     * You may find the following information useful:
     * Maximum 976 Records per page
     * B = 6
     *
     * @return Pair of leftRecords and rightRecords
     */
    public static Pair<List<Record>, List<Record>> getBreakNHJInputs() {
        ArrayList<Record> leftRecords = new ArrayList<>();
        ArrayList<Record> rightRecords = new ArrayList<>();
        for (int i = 0; i < 19521; i++) {
            leftRecords.add(createRecord(i));
        }

        rightRecords.add(createRecord(1));
        // TODO(proj3_part1): populate leftRecords and rightRecords such that NHJ breaks but not GHJ

        return new Pair<>(leftRecords, rightRecords);
    }

    /**
     * This method is called in testGHJBreak.
     *
     * Come up with two lists of records for leftRecords and rightRecords such that GHJ will error (in our case
     * hit the max pass cap).
     *
     * createRecord() takes in a value and returns a record with that value.
     *
     * Lists cannot be empty or the joins will error!
     *
     * You may find the following information useful:
     * Maximum 976 records per page
     * B = 6
     *
     * @return Pair of leftRecords and rightRecords
     */
    public static Pair<List<Record>, List<Record>> getBreakGHJInputs() {
        ArrayList<Record> leftRecords = new ArrayList<>();
        ArrayList<Record> rightRecords = new ArrayList<>();

        // TODO(proj3_part1): populate leftRecords and rightRecords such that GHJ breaks
        for (int i = 0; i < 976 * 4 + 1; i++) {
            leftRecords.add(createRecord(1));
        }
        for (int i = 0; i < 976 * 4 + 1; i++) {
            rightRecords.add(createRecord(1));
        }
        return new Pair<>(leftRecords, rightRecords);
    }
}

