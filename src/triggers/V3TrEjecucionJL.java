package triggers;

import org.h2.api.Trigger;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;

public class V3TrEjecucionJL implements Trigger {

    private static final Date D_1899 = Date.valueOf("1899-09-09");
    private static final Date D_1999 = Date.valueOf("1999-09-09");

    @Override
    public void init(Connection conn, String schemaName, String triggerName,
                     String tableName, boolean before, int type) {
        // no-op
    }

    private Integer asInt(Object v) {
        if (v == null) return null;
        if (v instanceof Number) return ((Number) v).intValue();
        String s = v.toString().trim();
        if (s.isEmpty()) return null;
        return Integer.valueOf(s);
    }

    private void setIfNull(Object[] newRow, int idx, Object value) {
        if (newRow[idx] == null) newRow[idx] = value;
    }

    private Date asDate(Object v) {
        if (v == null) return null;
        if (v instanceof Date) return (Date) v;
        if (v instanceof java.util.Date) return new Date(((java.util.Date) v).getTime());
        String s = v.toString().trim();
        if (s.isEmpty()) return null;
        return Date.valueOf(s); // espera yyyy-mm-dd
    }

    private void replace1999With1899(Object[] newRow, int idxDate) {
        Date d = asDate(newRow[idxDate]);
        if (d != null && d.equals(D_1999)) newRow[idxDate] = D_1899;
    }

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {

        // ===== Índices (0-based) según tu CREATE TABLE =====
        final int iFECHA_APERTURA_EXPEDIENTE = 3;
        final int iMOTIVO_SOLICITUD_EJ     = 4;
        final int iFECHA_PRESENTACION     = 5;
        final int iESTATUS_EXPE           = 6;
        final int iFECHA_CONCLUSION       = 7;
        final int iFASE_CONCLUSION        = 8;

        // ===== Defaults =====
        setIfNull(newRow, iMOTIVO_SOLICITUD_EJ, 9);
        setIfNull(newRow, iFECHA_PRESENTACION, D_1899);
        setIfNull(newRow, iESTATUS_EXPE, 9);

        // ===== Reglas por estatus =====
        Integer estatus = asInt(newRow[iESTATUS_EXPE]);
        if (estatus != null && estatus == 1) {
            setIfNull(newRow, iFECHA_CONCLUSION, D_1899);
            setIfNull(newRow, iFASE_CONCLUSION, 9);
        }

        // ===== Normalización 09/09/1999 -> 1899-09-09 =====
        replace1999With1899(newRow, iFECHA_APERTURA_EXPEDIENTE);
        replace1999With1899(newRow, iFECHA_PRESENTACION);
        replace1999With1899(newRow, iFECHA_CONCLUSION);
    }

    @Override public void close() { }
    @Override public void remove() { }
}
