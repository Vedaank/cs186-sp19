package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.BaseTransaction;

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
 * be given the X lock on db, and put in an unblocked state via BaseTransaction#unblock).
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

        // You may add helper methods here if you wish
    }

    // You should not modify or use this directly.
    protected Map<Object, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to NAME.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    // You may add helper methods here if you wish

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
     * isn't being released
     * @throws NoLockHeldException if no lock on a name in RELEASELOCKS is held by TRANSACTION
     */
    public void acquireAndRelease(BaseTransaction transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseLocks)
    throws DuplicateLockRequestException, NoLockHeldException {
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block -- in fact,
        // you will have to write some code outside the synchronized block to avoid locking up
        // the entire lock manager when a transaction is blocked. You are also allowed to
        // move the synchronized block elsewhere if you wish.
        synchronized (this) {
            // throw new UnsupportedOperationException("TODO(hw5): implement");
            // some local variables
            ResourceEntry resEntry = getResourceEntry(name);
            Long transNum = transaction.getTransNum();
            List<Lock> locks = transactionLocks.get(transNum);

            // check for duplicate lock request
            if (locks != null) {
                for (int i = 0; i < locks.size(); i++) {
                    if (locks.get(i).name.equals(name)) {
                        boolean dupLock = false;
                        for (ResourceName resourceName : releaseLocks) {
                            if (resourceName.equals(name)) {
                                dupLock = true;
                                break;
                            }
                        }
                        if (!dupLock) {
                            throw new DuplicateLockRequestException("Duplicate lock request");
                        }
                    }
                }
            }

            // check if acquire is possible
            int acquireType = getAcquireType(resEntry, lockType, transNum);

            // acquire
            if (acquireType == 1) {
                Lock transLock = new Lock(name, lockType, transNum);
                List<Lock> transLocks = transactionLocks.getOrDefault(transNum, new ArrayList<Lock>());
                transLocks.add(transLock);
                transactionLocks.put(transNum, transLocks);
                resEntry.locks.add(transLock);
                // release locks
                for (ResourceName resName: releaseLocks) {
                    release(transaction, resName);
                }
            } else if (acquireType == 2) {
                promote(transaction, name, lockType);
                if (releaseLocks.size() == 1) {
                    releaseLocks = new ArrayList<>();
                } else {
                    releaseLocks.remove(name);
                }
                // release locks
                for (ResourceName resName: releaseLocks) {
                    release(transaction, resName);
                }
            } else {
                List<Lock> resLocks = new ArrayList<>();
                for (ResourceName resName: releaseLocks) {
                    Lock curLock = new Lock(resName, getLock(transaction, name), transNum);
                    resLocks.add(curLock);
                }
                transaction.block();
                Lock requestLock = new Lock(name, lockType, transNum);
                LockRequest lockRequest = new LockRequest(transaction, requestLock, resLocks);
                resEntry.waitingQueue.addFirst(lockRequest);
            }
        }
    }

    // helper for acquire and release
    private LockType getLock(BaseTransaction transaction, ResourceName name){
        List<Lock> locks = transactionLocks.get(transaction.getTransNum());
        for(Lock lock: locks) {
            ResourceName resName = lock.name;
            if (resName.equals(name)){
                return lock.lockType;
            }
        }
        return LockType.NL;
    }

    // check if acquire is possible
    public int getAcquireType(ResourceEntry resEntry, LockType lockType, Long transNum) {
        for (Lock lock: resEntry.locks) {
            if (!LockType.compatible(lock.lockType, lockType)
                    && lock.transactionNum != transNum) {
                return -1;
            } else if (lock.transactionNum == transNum
                    && LockType.substitutable(lockType, lock.lockType)) {
                return 2;
            }
        }
        return 1;
    }

    /**
     * Helper Method for acquireAndRelease()
     * Checks that for every resource in releaseLocks, transaction actually has a lock on it.
     **/
    private void checkNoLockHeldExc(BaseTransaction transaction, List<ResourceName> releaseLocks){
        // checks to make sure there is lock on every resource in releaseLocks
        // for each lock to be released, checks to see if t
        Long transNum = transaction.getTransNum();
        List<Lock> transLocks = transactionLocks.get(transNum);
        for (int i = 0; i < releaseLocks.size(); i++) {
            ResourceName res = releaseLocks.get(i);
            //keep inside for loop since if there is no releaseLocks, it deosn't matter if there
            // are translocks or not
            if (transLocks == null){
                throw new NoLockHeldException("No Lock On this resource by this Xact");
            }
            //for this resource, check if this TRANSACTION has a lock on it
            if (!transContainLock(transaction, res)){
                throw new NoLockHeldException("No lock on this resource by this Xact");
            }
        }
    }

    /**
     * Helper method for checkNoLockHedExc
     * @return if TRANSACTION contains a lock on resource NAME then return true
     */
    private boolean transContainLock(BaseTransaction transaction, ResourceName name) {
        List<Lock> transLocks =  transactionLocks.get(transaction.getTransNum());
        for (Lock lock: transLocks) {
            ResourceName lockName = lock.name;
            if (lockName.equals(lockName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by
     * TRANSACTION
     */
    public void acquire(BaseTransaction transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block -- in fact,
        // you will have to write some code outside the synchronized block to avoid locking up
        // the entire lock manager when a transaction is blocked. You are also allowed to
        // move the synchronized block elsewhere if you wish.
        synchronized (this) {
            // throw new UnsupportedOperationException("TODO(hw5_part1): implement");
            // some local variables
            ResourceEntry resEntry = getResourceEntry(name);
            Long transNum = transaction.getTransNum();
            List<Lock> locks = transactionLocks.get(transNum);
            Lock lock = new Lock(name, lockType, transNum);

            // check for duplicate lock request
            // make sure locks is not null
            if (locks != null) {
                for (Lock resLock: locks) {
                    ResourceName resName = resLock.name;
                    if (resName.equals(name)) {
                        throw new DuplicateLockRequestException("Duplicate lock request");
                    }
                }
            }

            // see if acquire is possible
            boolean acquirable = true;
            for (Lock resLock: resEntry.locks) {
                if (!LockType.compatible(resLock.lockType, lockType)) {
                    acquirable = false;
                    break;
                }
            }

            // acquire
            if (acquirable && getResourceEntry(name).waitingQueue.isEmpty()){
                List<Lock> transLocks = transactionLocks.getOrDefault(transNum, new ArrayList<Lock>());
                transLocks.add(lock);
                transactionLocks.put(transNum, transLocks);
                resEntry.locks.add(lock);
            } else {
                transaction.block();
                LockRequest lockRequest = new LockRequest(transaction, lock);
                resEntry.waitingQueue.addLast(lockRequest);
            }
        }
    }

    /**
     * Release TRANSACTION's lock on NAME.
     *
     * Error checking must be done before the lock is released.
     *
     * NAME's queue should be processed after this call. If any requests in
     * the queue have locks to be released, those should be released, and the
     * corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     */
    public void release(BaseTransaction transaction, ResourceName name)
    throws NoLockHeldException {
        // You may modify any part of this method.
        synchronized (this) {
            // throw new UnsupportedOperationException("TODO(hw5_part1): implement");
            // some local variables
            ResourceEntry resEntry = resourceEntries.get(name);
            Long transNum = transaction.getTransNum();
            int transIndex = -1;
            int resEntIndex = -1;

            // check if resEntry is null
            if (resEntry == null) {
                throw new NoLockHeldException("No lock held");
            }

            // check if transaction locks held
            for (int i = 0; i < transactionLocks.get(transNum).size(); i++){
                Lock lock =transactionLocks.get(transNum).get(i);
                if (lock.name.equals(name)){
                    transIndex = i;
                    break;
                }
            }

            // check if resource entry locks held
            for (int i = 0; i < resEntry.locks.size(); i++){
                Lock lock = resEntry.locks.get(i);
                if (lock.transactionNum.equals(transNum)){
                    resEntIndex = i;
                }
            }

            // throw exception if lock held
            if (transIndex == -1 || resEntIndex == -1){
                throw new NoLockHeldException("No lock held");
            }

            transactionLocks.get(transNum).remove(transIndex);
            resEntry.locks.remove(resEntIndex);
            Deque<LockRequest> waitingQueue = getResourceEntry(name).waitingQueue;

            while (!waitingQueue.isEmpty()){
                LockRequest lockRequest = waitingQueue.peek();
                BaseTransaction newTrans = lockRequest.transaction;
                Long newTransNum = newTrans.getTransNum();
                LockType newLockType = lockRequest.lock.lockType;

                // check if acquire is possible
                int acquireType = getAcquireType(resEntry, newLockType, newTransNum);

                if (acquireType > 0) {
                    waitingQueue.poll();
                    if (acquireType == 1) {
                        // acquire
                        Lock transLock = new Lock(name, newLockType, newTransNum);
                        List<Lock> locks = transactionLocks.getOrDefault(newTransNum, new ArrayList<>());
                        locks.add(transLock);
                        transactionLocks.put(newTransNum, locks);
                        getResourceEntry(name).locks.add(transLock);
                    } else if (acquireType == 2) {
                        // promote
                        promote(newTrans, name, newLockType);
                    } else if (!lockRequest.releasedLocks.isEmpty()){

                        // acquire and release
                        List<ResourceName> release = new ArrayList<>();
                        for (Lock lock : lockRequest.releasedLocks) {
                            release.add(lock.name);
                        }
                        acquireAndRelease(newTrans, name, newLockType, release);
                    }
                    newTrans.unblock();
                } else {
                    return;
                }
            }
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
     * @throws InvalidLockException if the requested lock type is not a promotion. A promotion
     * from lock type A to lock type B is valid if and only if B is substitutable
     * for A, and B is not equal to A.
     */
    public void promote(BaseTransaction transaction, ResourceName name,
                        LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // You may modify any part of this method.
        synchronized (this) {
            // throw new UnsupportedOperationException("TODO(hw5_part1): implement");
            // some local variables
            ResourceEntry resEntry = getResourceEntry(name);
            Long transNum = transaction.getTransNum();
            List<Lock> locks = transactionLocks.get(transNum);

            // check if locks is null
            boolean lockHeld = false;
            if (locks == null) {
                throw new NoLockHeldException("No lock held");
            }

            // check if lock is held
            for (Lock lock: locks) {
                if (lock.name.equals(name)) {
                    lockHeld = true;
                }
            }
            if (!lockHeld) {
                throw new NoLockHeldException("No lock held");
            }

            // check if duplicate lock request
            for (int i = 0; i < locks.size(); i++) {
                if (locks.get(i).name.equals(name)
                        && locks.get(i).lockType.equals(newLockType)) {
                    throw new DuplicateLockRequestException("Duplicate lock request");
                }
            }

            // check invalid lock
            boolean promotable = false;
            int index = -1;
            for (int i = 0; i < resEntry.locks.size(); i++) {
                Lock lock = resEntry.locks.get(i);
                if (lock.transactionNum == transNum) {
                    // is promotion possible?
                    if (LockType.substitutable(newLockType, lock.lockType)) {
                        promotable = true;
                        index = i;
                    } else {
                        // invalid lock
                        throw new InvalidLockException("Invalid lock");
                    }
                } else if (!LockType.compatible(lock.lockType, newLockType)) {
                    // add to beginning of queue
                    transaction.block();
                    Lock requestLock = new Lock(name, newLockType, transNum);
                    LockRequest lockRequest = new LockRequest(transaction, requestLock);
                    resEntry.waitingQueue.addFirst(lockRequest);
                    return;
                }
            }

            // promote
            if (promotable) {
                getResourceEntry(name).locks.get(index).lockType = newLockType;
            }
        }
    }

    /**
     * Return the type of lock TRANSACTION has on NAME (return NL if no lock is held).
     */
    public synchronized LockType getLockType(BaseTransaction transaction, ResourceName name) {
        // throw new UnsupportedOperationException("TODO(hw5_part1): implement");
        // get locks
        List<Lock> locks = transactionLocks.get(transaction.getTransNum());
        if (locks == null) {
            return LockType.NL;
        }
        for (Lock lock: locks) {
            if (lock.name.equals(name)) {
                return lock.lockType;
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
    public synchronized List<Lock> getLocks(BaseTransaction transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                Collections.emptyList()));
    }

    /**
     * Create a lock context for the database. See comments at
     * the top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        contexts.putIfAbsent("database", new LockContext(this, null, "database"));
        return contexts.get("database");
    }

    /**
     * Create a lock context with no parent. Cannot be called "database".
     */
    public synchronized LockContext orphanContext(Object name) {
        if (name.equals("database")) {
            throw new IllegalArgumentException("cannot create orphan context named 'database'");
        }
        contexts.putIfAbsent(name, new LockContext(this, null, name));
        return contexts.get(name);
    }
}