package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.BaseTransaction;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock acquisition
 * for the user (you, in the second half of Part 2). Generally speaking, you should use LockUtil
 * for lock acquisition instead of calling LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that TRANSACTION can perform actions requiring LOCKTYPE on LOCKCONTEXT.
     * <p>
     * This method should promote/escalate as needed, but should only grant the least
     * permissive set of locks needed.
     * <p>
     * lockType must be one of LockType.S, LockType.X, and behavior is unspecified
     * if an intent lock is passed in to this method (you can do whatever you want in this case).
     * <p>
     * If TRANSACTION is null, this method should do nothing.
     */
    public static void ensureSufficientLockHeld(BaseTransaction transaction, LockContext lockContext,
                                                LockType lockType) {
        // throw new UnsupportedOperationException("TODO(hw5_part2): implement");
        if (transaction == null) {
            return;
        }

        if (lockContext.parent != null
                && lockContext.parent.saturation(transaction) >= .2
                && lockContext.parent.capacity() >= 10) {
            lockContext.parent.escalate(transaction);
            return;
        }

        if (lockType.equals(LockType.IS)) {
            lockType = LockType.S;
        } else if (lockType.equals(LockType.IX)) {
            lockType = LockType.X;
        }

        if (LockType.substitutable(lockContext.getEffectiveLockType(transaction), lockType)) {
            return;
        }
        checkLocks(transaction, lockContext, lockType);
    }

    private static void checkLocks(BaseTransaction transaction, LockContext lockContext,
                                   LockType lockType) {
        if (lockContext == null) {
            return;
        }
        LockType lock = lockContext.lockman.getLockType(transaction, lockContext.name);
        //if current lockcontext has correct lock, then we are done.
        if (LockType.substitutable(lock, lockType)) {
            return;
        }
        //recurse on parent
        checkLocks(transaction, lockContext.parent, LockType.parentLock(lockType));

        try {
            if (lockContext.getExplicitLockType(transaction).equals(LockType.NL)) {
                lockContext.acquire(transaction, lockType);
            } else {
                lockContext.promote(transaction, lockType);
            }
        } catch (Exception e) {
            lockContext.escalate(transaction);
            if (LockType.substitutable(lockType, lockContext.getEffectiveLockType(transaction))) {
                return;
            }
        }
    }
}
