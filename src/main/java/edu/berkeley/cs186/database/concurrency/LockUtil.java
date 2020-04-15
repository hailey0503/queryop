package edu.berkeley.cs186.database.concurrency;
// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!
import edu.berkeley.cs186.database.TransactionContext;

import java.util.List;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock acquisition
 * for the user (you, in the second half of Part 2). Generally speaking, you should use LockUtil
 * for lock acquisition instead of calling LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring LOCKTYPE on LOCKCONTEXT.
     *
     * This method should promote/escalate as needed, but should only grant the least
     * permissive set of locks needed.
     *
     * lockType is guaranteed to be one of: S, X, NL.
     *
     * If the current transaction is null (i.e. there is no current transaction), this method should do nothing.
     */
    // If the current lock permissions you have are sufficient, then you do not have to do anything.
    //grant as little additional permission as possible:
    // if an S lock suffices, we should have the transaction acquire an S lock, not an X lock, but if the transaction already has an X lock, we should leave it alone
    //  ensuring that we have the appropriate locks on ancestors, and acquiring the lock on the resource.
    //  You will need to promote in some cases, and escalate in some cases (these cases are not mutually exclusive).
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType lockType) {
        // TODO(proj4_part2): implement

        TransactionContext transaction = TransactionContext.getTransaction(); // current transaction
        if (transaction == null) {
            return;
        }
        LockType effectiveLock = lockContext.getEffectiveLockType(transaction); ////use this
        LockType explicitLock = lockContext.getExplicitLockType(transaction);
        LockType parentLock = null; //parent lock type
        LockContext parentContext = null;
        Boolean hasParent = false;
        LockType potentialParentLock = null;

        if (lockContext.parentContext() != null) {
            //doesn't actually get parent locktype ????
            parentContext = lockContext.parentContext(); //actual lock type on the context<---use this for checking parent type
            parentLock = parentContext.getEffectiveLockType(transaction);
            hasParent = true;
        }

        if (effectiveLock.equals(lockType)) { //but which lock (effective or explicit) to use?
            return;
        }
        if (lockType.equals(LockType.NL)) {
            return;
        }

        if (effectiveLock.equals(LockType.NL)) {
            if (LockType.canBeParentLock(parentLock, lockType)) {
                lockContext.acquire(transaction, lockType);
                return;
            } else {
                potentialParentLock = LockType.parentLock(lockType);
                if (parentLock == LockType.NL) {
                    updateParent(transaction, potentialParentLock, parentContext);
                    lockContext.acquire(transaction, lockType);
                    return;
                } else{
                    changeParent(transaction, potentialParentLock, parentContext);
                    lockContext.acquire(transaction, lockType);
                    return;
                }

            }

             //do we have to care the parent? ex, locktype X, parent(table) S then need SIX or locktype X for page level, but S at db.
        }

       /* if (LockType.substitutable(effectiveLock, lockType)) {
            return;
        }*/
        if (LockType.substitutable(lockType, effectiveLock)) {
            if (hasParent) {
                potentialParentLock = LockType.parentLock(lockType);
                changeParent(transaction, potentialParentLock, parentContext);
            }
            lockContext.promote(transaction, lockType);
            return;
        } else {
            if (effectiveLock.equals(LockType.SIX)) {
                if (lockType == LockType.X) { //SIX->X X is more permissive?
                    //checking parent
                    //changeParent(transaction, parentLock, lockContext.parentContext());
                    //lockContext.promote(transaction, lockType);
                    return;
                    }
                    // lockType S -> SIX should remain the same.
                    else if (lockType == LockType.S) {
                        return;
                    }
                }
                // S lock on the database, and then request an X lock on a page? Should this turn the S lock into a SIX lock and grant the X lock to the page?
                // how do we take care of parents?

                // every time curr lock type is promoted, check parent by canBeParent then change the parent if it can't be parent of the lock type just promoted?
                //what about escalate??
            if (effectiveLock.equals(LockType.S)) {
                if (lockType == LockType.X) {
                    if (hasParent) {
                        potentialParentLock = LockType.parentLock(lockType);
                        changeParent(transaction, potentialParentLock, parentContext);
                    }
                    lockContext.promote(transaction, lockType);
                    return;
                }
            }
            if (effectiveLock == LockType.X) {
                return;
            }
            if (effectiveLock == LockType.IS) {
                if (lockType == LockType.X) {
                    if (hasParent) {
                        potentialParentLock = LockType.parentLock(lockType);
                        changeParent(transaction, potentialParentLock, parentContext);
                    }
                    lockContext.promote(transaction, lockType);
                    return;
                } else if (lockType == LockType.S) {
                    lockContext.escalate(transaction);
                    return;
                }
            }
            // lockType S ->IX should become SIX.
            if (effectiveLock == LockType.IX) {
                if (lockType == LockType.S) {
                    if (hasParent) {
                        //if there is X on the child change to SIX
                        List<Lock> allLocks = lockContext.lockman.getLocks(transaction);
                        for (Lock l : allLocks) {
                            if (l.name.isDescendantOf(lockContext.name) || l.name.equals(lockContext.name)) { //need to check this level too?
                                if (l.lockType.equals(LockType.X)) {
                                    //changeParent(transaction, LockType.SIX, parentContext);
                                    lockContext.promote(transaction, LockType.SIX);//six???
                                    return;
                                }
                            }
                        }
                        potentialParentLock = LockType.parentLock(lockType);
                        changeParent(transaction, potentialParentLock, parentContext);
                    }
                    lockContext.promote(transaction, LockType.SIX);
                    return;
                } else if (lockType == LockType.X) {
                    lockContext.escalate(transaction);
                    return;
                }
            }
        }
    }

    // X is more permissive than SIX, but you can't have IS(table) and IX(page1) because IS cannot be parent of IX.
    //
    //@Anon. Calc 2, which part of the spec are you referring to? Because S and X cannot have descendant locks, you will have to call escalate most of the times in LockUtil.
    // X is more permissive than SIX, as X is more permissive than S and IX (and SIX is just the combination of the two) so your promotion idea also seems to be correct
    public static void updateParent(TransactionContext transaction, LockType lock, LockContext lockContext) {
        if (lockContext == null) {
            return;
        }
        LockType thisType = lockContext.getEffectiveLockType(transaction); //IS

        if (!thisType.equals(LockType.NL)) {
            return;
        }

        updateParent(transaction, lock, lockContext.parentContext());
        lockContext.acquire(transaction, lock);

    }

    public static void changeParent(TransactionContext transaction, LockType lock, LockContext lockContext) {
        if (lockContext == null) {
            return;
        } else {
            if (!LockType.substitutable(lock, lockContext.getEffectiveLockType(transaction))) {
                return;
            }
            changeParent(transaction, lock, lockContext.parentContext());
            lockContext.promote(transaction, lock);
        }
    }

    // TODO(proj4_part2): add helper methods as you see fit
}
