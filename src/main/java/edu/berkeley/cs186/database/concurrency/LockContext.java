package edu.berkeley.cs186.database.concurrency;
// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!
import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.table.Table;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;
    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;
    // The name of the resource this LockContext represents.
    protected ResourceName name;
    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;
    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;
    // The number of children that this LockContext has, if it differs from the number of times
    // LockContext#childContext was called with unique parameters: for a table, we do not
    // explicitly create a LockContext for every page (we create them as needed), but
    // the capacity should be the number of pages in the table, so we use this
    // field to override the return value for capacity().
    protected int capacity;

    protected boolean autoEscalate;

    // You should not modify or use this directly.
    protected final Map<Long, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, Pair<String, Long> name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, Pair<String, Long> name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.capacity = -1;
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
        this.autoEscalate = true;

    }

    /**
     * Gets a lock context corresponding to NAME from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<Pair<String, Long>> names = name.getNames().iterator();
        LockContext ctx;
        Pair<String, Long> n1 = names.next();
        ctx = lockman.context(n1.getFirst(), n1.getSecond());
        while (names.hasNext()) {
            Pair<String, Long> p = names.next();
            ctx = ctx.childContext(p.getFirst(), p.getSecond());
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a LOCKTYPE lock, for transaction TRANSACTION.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by TRANSACTION
     * @throws UnsupportedOperationException if context is readonly
     */
    // A transaction is allowed to have multiple locks each on a different resource, but not allowed to have multiple locks on one resource.
    // As you suspected, this error is checking to see if the transaction is trying to acquire a lock on a resource it already has a lock on.

    //in acquire am i understanding multigranularity contraints,
    // do I basically just check the two cases they gave us in the spec or am I supposed to do something else? Or do I simply just call checkCompatible?

    //To Anon. Comp 2, I would definitely check out the multigranularity guide for more information, or you can draw on your knowledge from Module 9.
    // The multi-granularity constraints are related to making sure that locks (whether they be S, X, IS, IX, or SIX)
    // on different levels of the database (tuple, page, table, database) are interacting with each other correctly;
    // a brief example is that if a transaction has an IS lock on the database, it can't have an X lock on one of the tables.
    // That would be a violation of a multigranularity constraint. This is where testing parent locks would come in handy.

    //In acquire, when we're checking if the lock is compatible with it's parent lock, should we be using getExplicitLockType or getEffectiveLockType on the parent?

    // At this point of the project, you should always be using the explicit lock types. Effective lock type is mainly useful for LockUtil
    // (which is why we put it at the end of the LockContext functions).
    //You shouldn't be checking compatible with parent lock, though. Compatible checks locks on the same resource.

    //Would checking if it can be parent lock for every ancestor, all the way to the root, be a good approach?
    //For acquire, a good place to start is checking if the requested lock can be granted given its parent lock.
    // In most lockType cases, any locks your parent context has should give you that information.
    // However, you should think about cases where you would want to check every ancestor.
    // Some of the helper functions we provide you in LockContext should be useful.
    private void addChild(TransactionContext transaction, LockContext parent) {
        if (parent == null) {
            return;
        }
        Long transNum = transaction.getTransNum();
        int numchildren = parent.numChildLocks.getOrDefault(transNum, 0);
        parent.numChildLocks.put(transNum, numchildren + 1);
        //addChildren(transaction, parent.parent);
    }

    public void acquire(TransactionContext transaction, LockType lockType)
    throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement
        LockType parentLockType = LockType.NL;
        Long transNum = transaction.getTransNum();
        if (parent != null) {
            parentLockType = parent.getEffectiveLockType(transaction);  //4/10
        }
        if (readonly) {
            throw new UnsupportedOperationException("Unsupported Operation.");
        }

        if (lockman.getLock(transNum, lockman.getLocks(name)) != null) {
            throw new DuplicateLockRequestException("Duplicate Request.");
        }
        if (hasSIXAncestor(transaction)) {
            System.out.println(parentLockType + " parent");
            System.out.println(lockType + " curr");
            if (lockType.equals(LockType.IS) || lockType.equals(LockType.S)) {
                throw new InvalidLockException("Invalid lock request.");
            }
        }
        if (LockType.canBeParentLock(parentLockType, lockType) || parent == null) {
            lockman.acquire(transaction, name, lockType);
            addChild(transaction, this.parent);
        } else {
            System.out.println(parentLockType + " parent");
            System.out.println(lockType + " curr");
            throw new InvalidLockException("Invalid lock request.");
        }
    }

    /**
     * Release TRANSACTION's lock on NAME.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     * @throws InvalidLockException if the lock cannot be released (because doing so would
     *  violate multigranularity locking constraints)
     * @throws UnsupportedOperationException if context is readonly
     */

    // 1) no ancestor check --> if no children is held by the transNum or if the children that is held by transNum is compatible to parent
    // 2) only check the immediate children.
    // before we release, we can assume that all the descendant paths starting with this LockContext are valid
    // (otherwise we wouldn't have been able to acquire the locks in such a way in the first place).
    // When we release the LockContext, if the children are valid, then the rest of the descendant tree should be valid as well.
    //
    //and do we check the direct children in this case?
    // do we use parentLock() in lockType to see if the child lock requires the parent and go through each children and check

    public void release(TransactionContext transaction)
    throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        Long transNum = transaction.getTransNum();
        //ResourceName name = getResourceName();
        List<Lock> locks = lockman.getLocks(transaction);
        if (readonly) {
            throw new UnsupportedOperationException("This Resource is read only.");
        }
        for (Lock l : locks) {//Question: I am checking all locks regardless of parent/children/itself
            if (l.lockType.equals(LockType.NL)) {
                throw new NoLockHeldException("No Lock Held Exception");
            }
        }
        if (!numChildLocks.containsKey(transNum)) { //no children with this T
        //if (capacity < 1) { //no children at all
            lockman.release(transaction, name);
            removeChild(transaction, parent); //change parent's child num b/c one lock is released!
        } else if (numChildLocks.get(transNum) == 0) { ////necessary? //there is no children that is held by T // basically the same..no? Just in case..
            lockman.release(transaction, name);
            removeChild(transaction, parent);
        }
        else {
            throw new InvalidLockException("This lock cannot be released.");
        }
    }


    //from the test, we hold IS at database, S at table and try to release IS I thought we are trying releasing middle(?), not the top.
    private void removeChild(TransactionContext transaction, LockContext parent) {
        if (parent == null) {
            return;
        }
        Long transNum = transaction.getTransNum();
        int numchildren = parent.numChildLocks.getOrDefault(transNum, 0);
        parent.numChildLocks.put(transaction.getTransNum(), numchildren - 1);
        //removeChild(transaction, parent.parent);
    }

    /**
     * Promote TRANSACTION's lock to NEWLOCKTYPE. For promotion to SIX from IS/IX/S, all S,and
     * IS locks on descendants must be simultaneously released. The helper function sisDescendants
     * may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a NEWLOCKTYPE lock
     * @throws NoLockHeldException if TRANSACTION has no lock
     * @throws InvalidLockException if the requested lock type is not a promotion or promoting
     * would cause the lock manager to enter an invalid state (e.g. IS(parent), X(child)).
     * A promotion from lock type A to lock type B is valid if B is substitutable
     * for A and B is not equal to A, or if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    // I'm trying to release all locks returned by sisDescendents in one of the methods.
    // I presume that when I do this, I have to update the numChildLocks map for the applicable children of the current LockContext object.
    // Given that we aren't supposed to use the children map at all,
    // how exactly are we supposed to access the children of the current LockContext object to update their numChildLocks map?

    // Actually I just saw the fromResourceName method, should we be using this?

    public void promote(TransactionContext transaction, LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        Long transNum = transaction.getTransNum();
        if (readonly) {
            throw new UnsupportedOperationException("This resource is read only.");
        }
        if (getEffectiveLockType(transaction).equals(LockType.NL)) {
            throw new NoLockHeldException("This resource does not hold any lock.");
        }
        if (getEffectiveLockType(transaction).equals(newLockType)) {
            throw new DuplicateLockRequestException("Duplicate Request.");
        }
        LockType thisType = getEffectiveLockType(transaction);
        LockContext context = fromResourceName(lockman, name);
        boolean substituable = false;
        boolean promotable = false;
        boolean checkSIX = false;
        if (!thisType.equals(newLockType)) {
            promotable = true;
        }
        if (LockType.substitutable(newLockType, thisType)) {
            substituable = true;
        }
        if (newLockType.equals(LockType.SIX) && ((thisType.equals(LockType.IS) || thisType.equals(LockType.IX) || thisType.equals(LockType.S)))) {
            checkSIX = true;
        }
        if ((substituable && promotable || checkSIX)) {
            if (checkSIX) { //release all redundants on descendants
                List<ResourceName> sisDecs = sisDescendants(transaction);
                sisDecs.add(name);
                lockman.acquireAndRelease(transaction, name,
                        newLockType, sisDecs);
                //lockman.promote(transaction, name, newLockType);

                //to deal with the case that sisDecs is null,
                /* if (sisDecs == null) {
                    acquire(transaction,newLockType);
                } else {
                    lockman.acquireAndRelease(transaction, name, newLockType, sisDecs);
                }*/ //but then duplicate error in acquire!!!

            } else {
                lockman.promote(transaction, name, newLockType);
            }
        } else {
            throw new InvalidLockException("request is invalid.");

        }
    }

    /**
     * Escalate TRANSACTION's lock from descendants of this context to this level, using either
     * an S or X lock. There should be no descendant locks after this
     * call, and every operation valid on descendants of this context before this call
     * must still be valid. You should only make *one* mutating call to the lock manager,
     * and should only request information about TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *      IX(database) IX(table1) S(table2) S(table1 page3) X(table1 page5)
     * then after table1Context.escalate(transaction) is called, we should have:
     *      IX(database) X(table1) S(table2)
     *--> check resourceName
     * You should not make any mutating calls if the locks held by the transaction do not change
     * (such as when you call escalate multiple times in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all relevant contexts, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws NoLockHeldException if TRANSACTION has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */

    //We can only escalate to either an S or an X lock.  It seems like we have to consider the parent and the descendants when figuring out which type to escalate to.
    // My parent matrix has different values for who can be a parent to an S lock or an X lock specifically if the parent is an IS or an SIX.
    //Questions:
    //1. If I look at the descendants and see that I can escalate to an X lock but that escalating to an X lock would violate multigranularity constraints for the parent,
    // should I instead escalate to an S lock?
    //2, If I look at the descendants and see that I can only escalate to an S lock but escalating to an S lock would violate multigranularity constraints for the parent,
    // should I instead escalate to an X lock?
    //

    //Kevin Wang 6 days ago 1. Why would we escalate to an X if an S would work just fine? We want the least permissions possible.
    // If we are forced to escalate to an X, then consider why setting it to an X would never violate multigranularity constraints.
    //2. This would ever occur. If an S already violates constraints (which it shouldn't), then an X lock would definitely violate it as well.



    private void escalateLock(TransactionContext transaction, LockType locktype, List<ResourceName> releases) {
        lockman.acquireAndRelease(transaction, name, locktype, releases);
        //addChild(transaction, this);
    }

    //Also note that since we are only escalating to S or X, a transaction that only has IS(database) would escalate to S(database).
    //we don't allow escalating to intent locks (IS/IX/SIX).

    //     *      IX(database) IX(table1) S(table2) S(table1 page3) X(table1 page5)
    //     * then after table1Context.escalate(transaction) is called, we should have:
    //     *      IX(database) X(table1) S(table2)
    //     *--> check resource name!!
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement
        if (readonly) {
            throw new UnsupportedOperationException("This resource is read only.");
        }
        if (getExplicitLockType(transaction).equals(LockType.NL)) {
            throw new NoLockHeldException("This resource does not hold any lock.");
        }
        LockType thisType = getExplicitLockType(transaction);///???4/18
        List<Lock> lockList = lockman.getLocks(transaction);
        List<ResourceName> rName = new ArrayList<>();
        LockContext thisContext = fromResourceName(lockman, name);

        if (thisType.equals(LockType.X) || thisType.equals(LockType.S)) {
            if (numChildLocks.getOrDefault(transaction.getTransNum(), 0) == 0) {
                return;
            } else {
                for (Lock l : lockList) {
                    //LockContext lockContext = fromResourceName(lockman, l.name);
                    //if (l.lockType == LockType.S) {
                        if (l.name.isDescendantOf(name)) {
                            removeChild(transaction, thisContext);
                        }
                }
            }
        }
        if (thisType.equals(LockType.IS)) {
            //add the lock on the same context to release
            rName.add(name);
            for (Lock l : lockList) {
                //LockContext lockContext = fromResourceName(lockman, l.name);
                //if (l.lockType == LockType.S) {
                    if (l.name.isDescendantOf(name)) {
                        rName.add(l.name);
                        removeChild(transaction, thisContext);
                   }
            }
            //release the lock on the same context

            escalateLock(transaction, LockType.S, rName);
        }
        if (thisType.equals(LockType.IX) || thisType.equals(LockType.SIX)) {
            rName.add(name);
            for (Lock l : lockList) {
                    if (l.name.isDescendantOf(name)) {
                        rName.add(l.name);
                        removeChild(transaction, thisContext);
                        }
                    }
            escalateLock(transaction, LockType.X, rName);
            }
        //if the current context has IS, we escalate it to S.
        //If the current context has IX, we escalate it to X.
        // if the current context has SIX, we escalate it to X.
        // If the current context has S, we simply delete its descendant locks.
        // If the current context has X, we simply delete its descendant locks.

        // Jamie Gu: That looks fine to me.
        // Just make sure that you always need to delete descendant locks when you escalate from a non-S/X lock.
        // And when current lock is S/X, there shouldn't be any descendant locks. <????

    }

    /**
     * Gets the type of lock that the transaction has at this level, either implicitly
     * (e.g. explicit S lock at higher level implies S lock at this level) or explicitly.
     * Returns NL if there is no explicit nor implicit lock.
     */

    public LockType getEffectiveLockType(TransactionContext transaction) { ///??? NEED TO CHECK
        if (transaction == null) {
            return LockType.NL;
        }
        // TODO(proj4_part2): implement
        LockType eLockType = getExplicitLockType(transaction);//SIX, IX, IS, S, X, NL
        //curr=SIX
        if (eLockType.equals(LockType.IX)) {
            if (hasSIXAncestor(transaction)) {
                return LockType.SIX;
            } else {
                return LockType.IX;
            }
        }
        if (eLockType.equals(LockType.IS)) {
            return LockType.IS;
        }
        if (eLockType.equals(LockType.NL) && parent != null) {
            eLockType = parent.getEffectiveLockType(transaction);

            if (eLockType.equals(LockType.IS) || eLockType.equals(LockType.IX)) {
                return LockType.NL;
            }
            if (eLockType.equals(LockType.S)) {
                return LockType.S;
            }
            if (eLockType.equals(LockType.X)) {
                return LockType.X;
            }

        }
        return eLockType ;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    //parentContext(), wouldn't I just need to check for the parent's locktype until I find a six or hit a null? Where does transaction play in?
    // locks are associated with a specific transaction, so when you check locks on the parent context,
    // you need to make sure you're checking locks that the transaction holds on that resource at that level.
    //
    //edit: I noticed I made a typo; originally said locks are associated with a specific parent, meant locks are associated with a specific transaction.
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        if (parent == null) {
            return false;
        }
        LockContext parent = parentContext();
        List<Lock> parentLocks = parent.lockman.getLocks(transaction);
        for (Lock l : parentLocks) {
            if (l.name.equals(name)) {
                if (l.lockType.equals(LockType.SIX)) {
                    return true;
                }
            }
        }
        parent.hasSIXAncestor(transaction);
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or IS and are descendants of current context
     * for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction holds a S or IS lock.
     */

    //in sisDescendants what is the recommended way to get the descendants of a lock?
    // Right now I'm creating a new childContext(...) with the second entry of the lock's current name, and then getting the locks from the lockman of the new childContext
    // and the original transaction with lockman.getLocks(...) which doesn't seem right. Any tips/hints?
    //A: You may find the function ResourceName.isDescendantOf(ResourceName) here to be helpful. It will return true if the caller is a descendant of the argument.

    //  do we have to get all of those locks' children as well using BFS?
    //  -> If you are referring to the escalate function, you need to consider all descendants, not just immediate descendants.

    // how we could get a list of every possible resource to consider at each level???
    //
    // Instead of getting a list of every possible resource, I got the list of all locks that the transaction held with getLocks and checked
    // if each lock had a resourcename that was a descendant of the current resourcename and if it was a S/IS lock.

    //  I think getLocks(transaction) will return a list of all locks held by a transaction, including those locks held by 'all descendants'
    //  If you return a list of all locks held by a transaction, you will get all of the locks at any granularity that the transaction holds.
    //  The approach you would want to use differs depending on the method, but if you're looking specifically at escalate, escalate operates on a specific transaction's locks.
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        List<Lock> allLocks = lockman.getLocks(transaction);
        List<ResourceName> sisDesc = new ArrayList<>();
        for (Lock l : allLocks) {
            if (l.name.isDescendantOf(name)) {
                if (l.lockType.equals(LockType.S) || l.lockType.equals(LockType.IS)) {
                    sisDesc.add(l.name);
                }
            }
        }
        return sisDesc;
    }

    /**
     * Get the type of lock that TRANSACTION holds at this level, or NL if no lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) {
            return LockType.NL;
        }
        // TODO(proj4_part2): implement
        LockType explicitLockType = lockman.getLockType(transaction, name);
        return explicitLockType;
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this context
     * to be readonly. This is used for indices and temporary tables (where
     * we disallow finer-grain locks), the former due to complexity locking
     * B+ trees, and the latter due to the fact that temporary tables are only
     * accessible to one transaction, so finer-grain locks make no sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name NAME (with a readable version READABLE).
     */
    public synchronized LockContext childContext(String readable, long name) {
        LockContext temp = new LockContext(lockman, this, new Pair<>(readable, name),
                                           this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) {
            child = temp;
        }
        if (child.name.getCurrentName().getFirst() == null && readable != null) {
            child.name = new ResourceName(this.name, new Pair<>(readable, name));
        }
        return child;
    }

    /**
     * Gets the context for the child with name NAME.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name), name);
    }

    /**
     * Sets the capacity (number of children).
     */
    public synchronized void capacity(int capacity) {
        this.capacity = capacity;
    }

    /**
     * Gets the capacity. Defaults to number of child contexts if never explicitly set.
     */
    public synchronized int capacity() {
        return this.capacity < 0 ? this.children.size() : this.capacity;
    }

    /**
     * Gets the saturation (number of locks held on children / number of children) for
     * a single transaction. Saturation is 0 if number of children is 0.
     */
    public double saturation(TransactionContext transaction) {
        if (transaction == null || capacity() == 0) {
            return 0.0;
        }
        return ((double) numChildLocks.getOrDefault(transaction.getTransNum(), 0)) / capacity();
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }

    public synchronized boolean getAutoEscalate() {
        return autoEscalate;
    }
    public synchronized void setAutoEscalate(boolean bool) {
        autoEscalate = bool;
    }

    //there is an access to tablecontext in Table.java??
    // make this returning tableContext
}

