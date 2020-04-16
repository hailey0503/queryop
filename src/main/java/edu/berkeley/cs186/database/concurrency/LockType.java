package edu.berkeley.cs186.database.concurrency;

// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!

public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        if (a.equals(LockType.S)) {
            if (b == NL || b == IS || b == S) {
                return true;
            }
        }
        if (a == X) {
            if (b == NL) {
                return true;
            }
        }
        if (a == IS) {
            if (b == NL || b == IS || b == IX || b == S || b == SIX) {
                return true;
            }
        }
        if (a == IX) {
            if (b == NL || b == IS || b == IX) {
                return true;
            }
        }
        if (a == SIX) {
            if (b == NL || b == IS) {
                return true;
            }
        }
        if (a == NL) {
            if (b == NL || b == IS || b == IX || b == S || b == SIX || b == X) {
                return true;
            }
        }

        return false;
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) { //parentLockType == null || ???
            throw new NullPointerException("No Lock Held");
        }
        // TODO(proj4_part1): implement
      /*  if (parentLockType == S) {
            if (childLockType == IS || childLockType == NL) {
                return true;
            }
        }
        if (parentLockType == X) {
            if (childLockType == IX || childLockType == NL) {
                return true;
            }
        }*/
        if (parentLockType.equals(IS)) {
            if (childLockType.equals(IS) || childLockType.equals(S)|| childLockType.equals(NL)) {
                return true;
            }
        }
        if (parentLockType.equals(IX)) {
            if (childLockType.equals(IX) || childLockType.equals(X) || childLockType.equals(SIX) || childLockType.equals(NL) || childLockType.equals(IS) || childLockType.equals(S) ) {//IS??????
                return true;
            }
        }
        if (parentLockType.equals(SIX)) { //??
            if (childLockType.equals(IX) || childLockType.equals(X) || childLockType.equals(SIX) || childLockType.equals(NL)) {
                return true;
            }
        }

        if (parentLockType.equals(NL) || parentLockType.equals(SIX) || parentLockType.equals(S) || parentLockType.equals(X)) {
            if (childLockType.equals(NL)) {
                return true;
            }
        }

        return false;
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    //for canParentBeLock, the note 11 offers some insight into the protocol on page 13.
    //
    //"To get S or IS lock on a node, must hold IS or IX on parent node."
    //
    //However, to gain an S lock, would it also be okay for the parent to have an S lock,
    // as that feels like a stronger condition that having an IS? or would that not make sense..?
    //If a transaction holds an S lock on a node, then it has read access to that node and all of its descendants.
    // Thus, an S lock on any of that node's descendants is redundant.
    //Anything can be the parent of NL, including NL. I'll leave the S or X question to you to determine, but yes S and X can be parent of NL
    // You should be able to figure out just using the description of each locktype in the readme.
    // If you treat each lock as a set of permissions, then "A can substitute B" really just asks the question "are A's permissions a superset of B's"? To get you started:
    //
    //NL(R) Permissions: {} (empty set)
    //X(R) Permissions: {Read R, Write R, Read R's descendants, Write R's descendants}
    //S(R) Permissions: {Read R, Read R's descendants}
    //
    //From this much you should be able to show that X can substitute S since X's permissions are a superset of S's, and that anything can substitute NL
    // (every set is a superset of the empty set).
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        if (required.equals(substitute)) {
            return true;
        }
        if (substitute.equals(SIX)){
            if ((required.equals(S)) || required.equals(IS) || required.equals(IX)) {
                return true;
            }
        } //redundant
        if (required.equals(NL)) {
            return true;
        }
        /*if (substitute == IS) {
            if (required == IS || required == IX) {
                return true;
            }
        }*/
        if (substitute.equals(IX)) {
            if (required.equals(IS)) { //???
                return true;
            }
        }
        if (substitute.equals(X)) {
            if (required.equals(IX) || required.equals(S)) {
                return true;

            }
        }

        // You can substitute S with S, SIX, or X
        // You cannot substitute X with IS, IX, S, or SIX
        return false;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

