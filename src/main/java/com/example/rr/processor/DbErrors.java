package com.example.rr.processor;

import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.util.Set;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.TransactionSystemException;

/** "The database is unavailable" (outage, switchover, fenced account) as opposed to "this record is bad". */
public final class DbErrors {

    /**
     * ORA-00028 session killed, 01012 not logged on, 01033/01034/01089 DB starting/unavailable/shutting down,
     * 03113/03114/03135 lost connection, 12514/12541 listener, 28000 account locked (fenced),
     * 16000 read-only (standby), 17002/17008/17410 JDBC I/O / closed connection.
     */
    private static final Set<Integer> UNAVAILABLE_ORA_CODES = Set.of(
            28, 1012, 1033, 1034, 1089, 3113, 3114, 3135, 12514, 12541, 28000, 16000, 17002, 17008, 17410);

    private DbErrors() {}

    public static boolean isUnavailable(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof DataAccessResourceFailureException
                    || c instanceof TransientDataAccessException
                    || c instanceof RecoverableDataAccessException
                    || c instanceof CannotCreateTransactionException
                    || c instanceof TransactionSystemException
                    || c instanceof SQLRecoverableException
                    || c instanceof SQLTransientException) {
                return true;
            }
            if (c instanceof SQLException sql && UNAVAILABLE_ORA_CODES.contains(sql.getErrorCode())) {
                return true;
            }
        }
        return false;
    }
}
