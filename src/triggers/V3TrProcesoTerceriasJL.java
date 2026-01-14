package triggers;

import org.h2.api.Trigger;
import java.sql.Connection;
import java.sql.Date;

public class V3TrProcesoTerceriasJL implements Trigger {

    // ===== Column indexes (AJUSTAR AL ORDEN REAL DE LA TABLA) =====
    private static final int FECHA_INCIDENTE = 1;
    private static final int FECHA_APERTURA_INCIDENTAL = 2;
    private static final int TIPO_INCIDENTE = 3;
    private static final int CELEBRACION_AUDIENCIA = 4;
    private static final int FECHA_AUDIENCIA = 5;
    private static final int ESTATUS_EXPEDIENTE = 6;
    private static final int SENTENCIA_INCIDENTAL = 7;
    private static final int FECHA_RESOLUCION = 8;

    private static final Date DEFAULT_DATE = Date.valueOf("1899-09-09");
    private static final Date BAD_DATE = Date.valueOf("1999-09-09");

    @Override
    public void init(Connection conn, String schemaName, String triggerName,
                     String tableName, boolean before, int type) {
        // no-op
    }

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) {

        int celebracion = toInt(newRow[CELEBRACION_AUDIENCIA], 0);
        int estatus = toInt(newRow[ESTATUS_EXPEDIENTE], 0);

        // ===== Fechas iniciales =====
        if (newRow[FECHA_INCIDENTE] == null)
            newRow[FECHA_INCIDENTE] = DEFAULT_DATE;

        if (newRow[FECHA_APERTURA_INCIDENTAL] == null)
            newRow[FECHA_APERTURA_INCIDENTAL] = DEFAULT_DATE;

        // ===== Tipo incidente =====
        if (newRow[TIPO_INCIDENTE] == null)
            newRow[TIPO_INCIDENTE] = 9;

        // ===== Celebración audiencia =====
        if (newRow[CELEBRACION_AUDIENCIA] == null)
            newRow[CELEBRACION_AUDIENCIA] = 9;

        if (celebracion == 1 && newRow[FECHA_AUDIENCIA] == null)
            newRow[FECHA_AUDIENCIA] = DEFAULT_DATE;

        // ===== Estatus expediente =====
        if (newRow[ESTATUS_EXPEDIENTE] == null)
            newRow[ESTATUS_EXPEDIENTE] = 9;

        if (estatus == 1 && newRow[SENTENCIA_INCIDENTAL] == null)
            newRow[SENTENCIA_INCIDENTAL] = 9;

        if (estatus == 1 && newRow[FECHA_RESOLUCION] == null)
            newRow[FECHA_RESOLUCION] = DEFAULT_DATE;

        // ===== Normalización de fechas incorrectas =====
        normalizeDate(newRow, FECHA_INCIDENTE);
        normalizeDate(newRow, FECHA_APERTURA_INCIDENTAL);
        normalizeDate(newRow, FECHA_AUDIENCIA);
        normalizeDate(newRow, FECHA_RESOLUCION);
    }

    private void normalizeDate(Object[] row, int idx) {
        if (row[idx] instanceof Date) {
            Date d = (Date) row[idx];
            if (BAD_DATE.equals(d)) {
                row[idx] = DEFAULT_DATE;
            }
        }
    }

    @Override public void close() {}
    @Override public void remove() {}

    // ===== Helper =====
    private static int toInt(Object v, int def) {
        if (v == null) return def;
        if (v instanceof Number) return ((Number) v).intValue();
        try { return Integer.parseInt(v.toString()); }
        catch (Exception e) { return def; }
    }
}
