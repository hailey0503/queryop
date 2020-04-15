package edu.berkeley.cs186.database.concurrency;
// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have
 * what locks on what resources. The lock manager should generally **not**
 * be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with
 * multiple levels of granularity (you can and should treat ResourceName
 * as a generic Object, rather than as an object encapsulating levels of
 * granularity, in this class).
 *
 * It follows that LockManager should allow **all**
 * requests that are valid from the perspective of treating every resource
 * as independent objects, even if they would be invalid from a
 * multigranularity locking perspective. For example, if LockManager#acquire
 * is called asking for an X lock on Table A, and the transaction has no
 * locks at the time, the request is considered valid (because the only problem
 * with such a request would be that the transaction does not have the appropriate
 * intent locks, but that is a multigranularity concern).
 *
 * Each resource the lock manager manages has its own queue of LockRequest objects
 * representing a request to acquire (or promote/acquire-and-release) a lock that
 * could not be satisfied at the time. This queue should be processed every time
 * a lock on that resource gets released, starting from the first request, and going
 * in order until a request cannot be satisfied. Requests taken off the queue should
 * be treated as if that transaction had made the request right after the resource was
 * released in absence of a queue (i.e. removing a request by T1 to acquire X(db) should
 * be treated as if T1 had just requested X(db) and there were no queue on db: T1 should
 * be given the X lock on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();
    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // Below are a list of helper methods you should implement!
        // Make sure to use these helper methods to abstract your code and
        // avoid re-implementing every time!

        /**
         * Check if a LOCKTYPE lock is compatible with preexisting locks.
         * Allows conflicts for locks held by transaction id EXCEPT.
         */

        boolean checkCompatible(LockType lockType, long except) {
            // TODO(proj4_part1): implement
            for (Lock lock : locks) {
                if (!lock.transactionNum.equals(except)) {
                    if (!LockType.compatible(lockType, lock.lockType)) {
                        return false;
                    }
                }
            }
            return true;
        }

        /**
         * Gives the transaction the lock LOCK. Assumes that the lock is compatible.
         * Updates lock on resource if the transaction already has a lock.
         */

        void grantOrUpdateLock(Lock lock) {
            // TODO(proj4_part1): implement
            Long transNum = lock.transactionNum;
            ResourceName newName = lock.name;
            LockType newLockType = lock.lockType;
            List<Lock> transLockList = transactionLocks.get(transNum);
            boolean bool = false;
            //empty arraylist
            //not contain
            //1. check over list of lock to see if name = lock name, change the locktype
            //every single time add that lock (passed in) to the list
            if (!transactionLocks.containsKey(transNum)) { //not contains key -> put key and val (new list) (grant)
                List<Lock> newLockList = new ArrayList<>();
                newLockList.add(lock);
                transactionLocks.put(transNum, newLockList);
                if (!locks.contains(lock)) { //can I assume there is no this lock in resourceEntry locks?
                    locks.add(lock);
                }
                return;
            }
            if (transactionLocks.containsKey(transNum)) {
                if (transLockList.isEmpty()) {
                    transLockList.add(lock); //containsKey but lockList is empty -> add the lock to locks (grant)
                    locks.add(lock); ////do we have to check if there is no this lock in locks?
                } else { //contains key and list is not empty
                    for (Lock rl : locks) { //containsKey and update lock type in locks
                        if (rl.transactionNum.equals(transNum)) {
                            if (rl.name.equals(newName)) {//check if they share the same resource..////same object
                                bool = true;
                                rl.lockType = newLockType;
                            }
                        }
                    }
                    for (Lock l : transLockList) { // update transLockList lockType
                        if (l.transactionNum.equals(transNum)) {
                            if (l.name.equals(newName)) {//check if they share the same resource..////same object
                                //bool = true;
                                l.lockType = newLockType;
                            }
                        }
                    }
                    if (bool == false) { //containsKey but the key's value lockList does not have this lock -> add
                        transLockList.add(lock);
                        locks.add(lock); //do we have to check if there is no this lock in locks?
                    }
                }
            }
        }

        /**
         * Releases the lock LOCK and processes the queue. Assumes it had been granted before.
         */

        void releaseLock(Lock lock) {
            // TODO(proj4_part1): implement
            Long transNum = lock.transactionNum;
            List<Lock> lockList = transactionLocks.get(transNum);
            lockList.remove(lock);
            locks.remove(lock);
            processQueue();
        }


        /**
         * Adds a request for LOCK by the transaction to the queue and puts the transaction
         * in a blocked state.
         */


        void addToQueue(LockRequest request, boolean addFront) {
            // TODO(proj4_part1): implement
            if (addFront) {
                waitingQueue.addFirst(request);
            } else {
                waitingQueue.addLast(request);
            }
        }

        /**
         * Grant locks to requests from front to back of the queue, stopping
         * when the next lock cannot be granted.
         */

        private void processQueue() {
            // TODO(proj4_part1): implement
            Iterator<LockRequest> waitingIter = waitingQueue.iterator();
            while (waitingIter.hasNext()) {
                LockRequest request = waitingIter.next();
                //System.out.println(waitingIter.next());
                Lock requestLock = request.lock;
                LockType requestLockType = requestLock.lockType;
                if (checkCompatible(requestLockType, request.lock.transactionNum)) {
                    grantOrUpdateLock(requestLock);
                    waitingIter.remove();
                    //locks.remove(request.lock);
                    for (Lock lockToRelease : request.releasedLocks) {
                        if (!lockToRelease.name.equals(requestLock.name)) { //One way to do this is to skip releasing a lock if its on the same ResourceName as the lock you just acquired.????
                            getResourceEntry(lockToRelease.name).releaseLock(lockToRelease);

                        }
                    }
                    request.transaction.unblock();
                }
                break;
                //request.transaction.unblock(); ///unblock
                }
            }



        /**
         * Gets the type of lock TRANSACTION has on this resource.
         */
        // You should be able to just loop through those locks and check if any of them belong to the transaction specified by the transactionNum passed in.
        // To check a Lock's transaction number you can access it's transactionNum field directly.
        LockType getTransactionLockType(long transaction) {
            // TODO(proj4_part1): implement
            for (Lock l : locks) {
                if (l.transactionNum.equals(transaction)) {
                    return l.lockType;
                }
            }
            return LockType.NL;
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                   ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<Long, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to NAME.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    // TODO(proj4_part1): You may add helper methods here if you wish

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION, and releases all locks
     * in RELEASELOCKS after acquiring the lock, in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * Locks in RELEASELOCKS should be released only after the requested lock has been acquired.
     * The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on NAME **should not** change the
     * acquisition time of the lock on NAME, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), acquire X(A) and release S(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by TRANSACTION and
     * isn't being released //lock is in that releaseLocks
     * @throws NoLockHeldException if no lock on a name in RELEASELOCKS is held by TRANSACTION
     */
    /* The list of transaction locks will be useful for keeping track of
    /* whether we can release a lock (check for ownership), duplicates, etc...

    /* Resource locks will be useful for managing conflicting locks on a given resource.
     */

    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseLocks)
    throws DuplicateLockRequestException, NoLockHeldException {
        boolean shouldBlock = false;
        synchronized (this) {
            Boolean bool = false;
            Long transNum = transaction.getTransNum();
            Lock newLock = new Lock(name, lockType, transNum);
            ResourceEntry entry = getResourceEntry(name);
            LockType thisLockType = getLockType(transaction, name);///to get resourceEntry. necessary??
            // TODO(proj4_part1): implement
            for (ResourceName reName : releaseLocks) { ///check if that lock in in the releaseLocks list
                if (reName.equals(name)) {
                    ResourceEntry reEntry = getResourceEntry(reName);
                    for (Lock rlock : reEntry.locks) {
                        if (rlock.equals(newLock)) {
                            bool = true;
                        }
                    }
                }
            }
            if (newLock.lockType != LockType.NL) {
                Lock rlock = getLock(transNum, entry.locks);
                if (rlock != null) {
                    if (rlock.lockType.equals(lockType) && bool == false) { //checking if there is exact same lock (should I just check if the name has a lock with same transcation?
                        throw new DuplicateLockRequestException("Duplicated!");
                    }
                }
            }
            //@throws NoLockHeldException if no lock on a name in RELEASELOCKS is held by TRANSACTION
            //It means no lock as in an "NL" lock, i.e. for each name in releaseLocks,
            // if the current transaction's lock on that name is an NL lock, then throw the exception.
            // If the releaseLock list is completely empty then you shouldn't throw the exception.
            if (!releaseLocks.isEmpty()) {
                for (ResourceName n : releaseLocks) {
                    if (getLockType(transaction, n) == LockType.NL) {
                            throw new NoLockHeldException("No Lock Held Exception");
                    }
                }
            }
            //If the new lock
            //     * is not compatible with another transaction's lock on the resource, the transaction is
            //     * blocked and the request is placed at the **front** of ITEM's queue.
            //     *
            //     * Locks in RELEASELOCKS should be released only after the requested lock has been acquired.
            //     * The corresponding queues should be processed.
            if (entry.checkCompatible(newLock.lockType, transNum)) {
                acquire(transaction, name, lockType); //X is updated then it is just released due to next line so I get NL instead of X
                //release

                for (ResourceName n : releaseLocks) {
                    if (!n.equals(name)) {
                        //check if name the same with release lock
                        release(transaction, n);
                    }
                }
            } else {
                LockRequest request = new LockRequest(transaction, newLock);
                entry.addToQueue(request, true);
                shouldBlock = true;
                transaction.prepareBlock();

            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION.
     *
     * Error checking must be done before the lock is acquired.
     * If the new lock
     * is not compatible with another transaction's lock on the resource,
     * or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by
     * TRANSACTION
     */

    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block -- in fact,
        // you will have to write some code outside the synchronized block to avoid locking up
        // the entire lock manager when a transaction is blocked. You are also allowed to
        // move the synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        synchronized (this) {
            Long transNum = transaction.getTransNum();
            Lock newLock = new Lock(name, lockType, transNum);
            ResourceEntry entry = getResourceEntry(name); ///replace with this
            Boolean bool = false;
            LockRequest request = new LockRequest(transaction, newLock);

            //@throws DuplicateLockRequestException if a lock on NAME is held by * TRANSACTION
            //
            Lock rlock = getLock(transNum, entry.locks);
            if (rlock != null) {
                if (rlock.lockType.equals(lockType)) {
                    throw new DuplicateLockRequestException("Duplicated!");
                }
            }

            if ((entry.waitingQueue.isEmpty()) && entry.checkCompatible(lockType, transNum)) { //add newLock
                entry.grantOrUpdateLock(newLock);
            } else {
                entry.addToQueue(request, false);
                shouldBlock = true;
                transaction.prepareBlock();
            }

        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Release TRANSACTION's lock on NAME.
     *
     * Error checking must be done before the lock is released. ///
     *
     * NAME's queue should be processed after this call. If any requests in
     * the queue have locks to be released, those should be released, and the
     * corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     */

    public void release(TransactionContext transaction, ResourceName name)
    throws NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        synchronized (this) {
            if (getLockType(transaction, name) == LockType.NL) {
                throw new NoLockHeldException("No Lock Held");
            }
            Boolean bool = false;
            ResourceEntry entry = getResourceEntry(name);
            Long transNum = transaction.getTransNum();
            LockType lockType = getLockType(transaction, name);
            Lock newLock = new Lock(name, lockType, transaction.getTransNum());
            entry.releaseLock(newLock);


        }
    }

    /**
     * Promote TRANSACTION's lock on NAME to NEWLOCKTYPE (i.e. change TRANSACTION's lock
     * on NAME from the current lock type to NEWLOCKTYPE, which must be strictly more
     * permissive).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * A lock promotion **should not** change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a
     * NEWLOCKTYPE lock on NAME
     * @throws NoLockHeldException if TRANSACTION has no lock on NAME
     * @throws InvalidLockException if the requested lock type is not a promotion.
     * A promotion
     * from lock type A to lock type B is valid if and only if B is substitutable
     * for A, and B is not equal to A.
     */


    public Lock getLock(long transNum, List<Lock> locks) {
        for (Lock l : locks) {
            if (l.transactionNum == transNum) {
                return l;
            }
        }
        return null;
    }
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part1): implement
        boolean shouldBlock = false;
        synchronized (this) {
            boolean bool = false;
            Long transNum = transaction.getTransNum();
            ResourceEntry entry = getResourceEntry(name);
            LockType currLockType = getLockType(transaction, name);
            Lock newLock = new Lock(name, newLockType, transNum);
            LockRequest request = new LockRequest(transaction, newLock);

            Lock currLock = getLock(transNum, entry.locks);
            //throw exceptions
            if (currLockType.equals(newLockType)) {
                throw new DuplicateLockRequestException("Duplicated");
            }
            // if TRANSACTION has no lock on NAME
            if (currLockType == LockType.NL) {
                throw new NoLockHeldException("No Lock Held");
            }
            if ((!LockType.substitutable(newLockType, currLockType))) {
                throw new InvalidLockException("InvalidLockException");
            }

            // If the new lock
            //     * is not compatible with another transaction's lock on the resource, the transaction is
            //     * blocked and the request is placed at the **front** of ITEM's queue.
            if (entry.checkCompatible(newLockType, transNum)) {
                if (LockType.substitutable(newLockType, currLockType)) {
                    Lock toReplaceLock = currLock;
                    toReplaceLock.lockType = newLockType;
                }
            } else {
                entry.addToQueue(request, true);
                shouldBlock = true;
                transaction.prepareBlock();

            }
        }
        if(shouldBlock){
            transaction.block();
        }
    }

    /**
     * Return the type of lock TRANSACTION has on NAME (return NL if no lock is held).
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        // TODO(proj4_part1): implement
        ResourceEntry entry = getResourceEntry(name);
        if (!entry.locks.isEmpty()) {
            for (Lock l : entry.locks) {
                if (l.transactionNum ==transaction.getTransNum()) {
                    return l.lockType;
                }
            }
        }
        return LockType.NL;
    }

    /**
     * Returns the list of locks held on NAME, in order of acquisition.
     * A promotion or acquire-and-release should count as acquired
     * at the original time.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks locks held by
     * TRANSACTION, in order of acquisition. A promotion or
     * acquire-and-release should count as acquired at the original time.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                               Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at
     * he top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext context(String readable, long name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, new Pair<>(readable, name)));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at
     * the top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database", 0L);
    }
}
