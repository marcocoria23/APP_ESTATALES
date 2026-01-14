package triggers;

import org.h2.api.Trigger;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;

public class V3TrProcesoEjecucionJL implements Trigger {

    // 🔴 AJUSTA ÍNDICES SEGÚN TU CREATE TABLE REAL
    private static final int COL_MOTIVO_SOLICITUD_EJ      = 1;
    private static final int COL_FECHA_PRESENTACION      = 2;
    private static final int COL_ESTATUS_EXPE             = 3;
    private static final int COL_FECHA_CONCLUSION         = 4;
    private static final int COL_FASE_CONCLUSION          = 5;
    private static final int COL_FECHA_APERTURA_EXPEDIENTE = 6;

    private static final Date FECHA_DEFAULT = Date.valueOf("1899-09-09");
    private static final Date FECHA_1999    = Date.valueOf("1999-09-09");

    @Override
    public void init(Connection conn, String schemaName, String triggerName,
                     String tableName, boolean before, int type) {
        // Sin inicialización
    }

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) {

        // === Defaults ===

        setIfNull(newRow, COL_MOTIVO_SOLICITUD_EJ, 9);
        setIfNull(newRow, COL_FECHA_PRESENTACION, FECHA_DEFAULT);
        setIfNull(newRow, COL_ESTATUS_EXPE, 9);

        // === Condiciones por estatus ===
        if (is(newRow, COL_ESTATUS_EXPE, 1)) {
            setIfNull(newRow, COL_FECHA_CONCLUSION, FECHA_DEFAULT);
            setIfNull(newRow, COL_FASE_CONCLUSION, 9);
        }

        // === Normalización de fechas 1999 → 1899 ===
        normalizeDate(newRow, COL_FECHA_APERTURA_EXPEDIENTE);
        normalizeDate(newRow, COL_FECHA_PRESENTACION);
        normalizeDate(newRow, COL_FECHA_CONCLUSION);
    }

    @Override public void close() {}
    @Override public void remove() {}

    // ===== Helpers =====

    private static void setIfNull(Object[] row, int idx, Object value) {
        if (row[idx] == null) {
            if (value instanceof Integer) {
                row[idx] = BigDecimal.valueOf((Integer) value);
            } else {
                row[idx] = value;
            }
        }
    }

    private static boolean is(Object[] row, int idx, int expected) {
        Object v = row[idx];
        if (v == null) return false;
        if (v instanceof BigDecimal) {
            return ((BigDecimal) v).intValue() == expected;
        }
        return Integer.parseInt(v.toString()) == expected;
    }

    private static void normalizeDate(Object[] row, int idx) {
        if (row[idx] instanceof Date) {
            Date d = (Date) row[idx];
            if (d.equals(FECHA_1999)) {
                row[idx] = FECHA_DEFAULT;
            }
        }
    }
}
