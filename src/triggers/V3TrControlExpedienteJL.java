package triggers;

import org.h2.api.Trigger;

import java.math.BigDecimal;
import java.sql.Connection;

public class V3TrControlExpedienteJL implements Trigger {

    // 🔴 AJUSTA LOS ÍNDICES SEGÚN EL CREATE TABLE REAL
    private static final int COL_CIRCUNS_ORG_JUR      = 1;
    private static final int COL_OTRO_ESP_CIRCUNS     = 2;
    private static final int COL_JURISDICCION         = 3;
    private static final int COL_ORDINARIO            = 4;
    private static final int COL_ESPECIAL_INDIVI      = 5;
    private static final int COL_ESPECIAL_COLECT      = 6;
    private static final int COL_HUELGA               = 7;
    private static final int COL_COL_NATU_ECONOMICA   = 8;
    private static final int COL_PARAP_VOLUNTARIO     = 9;
    private static final int COL_TERCERIAS            = 10;
    private static final int COL_PREF_CREDITO         = 11;
    private static final int COL_EJECUCION             = 12;
    private static final int COL_LATITUD_ORG           = 13;
    private static final int COL_LONGITUD_ORG          = 14;

    @Override
    public void init(Connection conn, String schemaName, String triggerName,
                     String tableName, boolean before, int type) {
        // No initialization needed
    }

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) {

        // === Valores por defecto ===

        setIfNull(newRow, COL_CIRCUNS_ORG_JUR, 9);

        if (is(newRow, COL_CIRCUNS_ORG_JUR, 4)) {
            setIfNull(newRow, COL_OTRO_ESP_CIRCUNS, "No Especifico");
        }

        setIfNull(newRow, COL_JURISDICCION, 9);

        setIfNull(newRow, COL_ORDINARIO, 0);
        setIfNull(newRow, COL_ESPECIAL_INDIVI, 0);
        setIfNull(newRow, COL_ESPECIAL_COLECT, 0);
        setIfNull(newRow, COL_HUELGA, 0);
        setIfNull(newRow, COL_COL_NATU_ECONOMICA, 0);
        setIfNull(newRow, COL_PARAP_VOLUNTARIO, 0);
        setIfNull(newRow, COL_TERCERIAS, 0);
        setIfNull(newRow, COL_PREF_CREDITO, 0);
        setIfNull(newRow, COL_EJECUCION, 0);

        setIfNull(newRow, COL_LATITUD_ORG, "No identificado");
        setIfNull(newRow, COL_LONGITUD_ORG, "No identificado");
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
}
