package edu.berkeley.cs186.database.concurrency;

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
        // throw new UnsupportedOperationException("TODO(hw5_part1): implement");
        boolean [][] compatiblility = new boolean[][]{
            {true, true, true, true, false, true},
            {true, true, false, false, false, true},
            {true, false, true, false, false, true},
            {true, false, false, false, false, true},
            {false, false, false, false, false, true},
            {true, true, true, true, true, true}
        };
        
        int x;
        int y;

        switch (a) {
            case S:
                x = 2;
                break;
            case X:
                x = 4;
                break;
            case IS:
                x = 0;
                break;
            case IX:
                x = 1;
                break;
            case SIX:
                x = 3;
                break;
            default:
                x = 5;
                break;
        }

        switch (b) {
            case S:
                y = 2;
                break;
            case X:
                y = 4;
                break;
            case IS:
                y = 0;
                break;
            case IX:
                y = 1;
                break;
            case SIX:
                y = 3;
                break;
            default:
                y = 5;
                break;
        }

        return compatiblility[x][y];
    }

    /**
     * This method returns the least permissive lock on the parent resource
     * that must be held for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        // throw new UnsupportedOperationException("TODO(hw5_part1): implement");
        switch (a) {
            case S:     return IS;
            case X:     return IX;          
            case SIX:   return IX;         
            case IX:    return IX;          
            case IS:    return IS;
            default:    return NL;
        }
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // throw new UnsupportedOperationException("TODO(hw5_part1): implement");
        boolean [][] substitutes = new boolean[][]{
            {true, false, false, false, false, true},
            {true, true, false, true, false, true},
            {true, false, true, false, false, true},
            {true, true, true, true, false, true},
            {true, true, true, true, true, true},
            {false, false, false, false, false, true}
        };

        int x;
        int y;

        switch (substitute) {
            case S:
                x = 2;
                break;
            case X:
                x = 4;
                break;
            case IS:
                x = 0;
                break;
            case IX:
                x = 1;
                break;
            case SIX:
                x = 3;
                break;
            default:
                x = 5;
                break;
        }

        switch (required) {
            case S:
                y = 2;
                break;
            case X:
                y = 4;
                break;
            case IS:
                y = 0;
                break;
            case IX:
                y = 1;
                break;
            case SIX:
                y = 3;
                break;
            default:
                y = 5;
                break;
        }
        return substitutes[x][y];
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
};

