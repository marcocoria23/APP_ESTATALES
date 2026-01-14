package triggers;

import org.h2.api.Trigger;
import java.sql.Connection;
import java.sql.Date;

public class V3TrProcesoPreferenciaCreditoJL implements Trigger {

    // ===== Column indexes (AJUSTAR AL ORDEN REAL DE LA TABLA) =====
    private static final int AVISO_ORGANO_JURIS = 1;
    private static final int AVISO_AUTORIDAD_ADMIN = 2;
    private static final int FECHA_PRESENTACION = 3;
    private static final int FECHA_ADMISION = 4;
    private static final int PROMOVENTE = 5;
    private static final int ESPECIFIQUE = 6;
    private static final int ESTATUS_EXPE = 7;
    private static final int FECHA_RESOLUCION = 8;
    private static final int FECHA_APERTURA_EXPEDIENTE = 9;

    private static final Date DEFAULT_DATE = Date.valueOf("1899-09-09");
    private static final Date BAD_DATE = Date.valueOf("1999-09-09");

    @Override
    public void init(Connection conn, String schemaName, String triggerName,
                     String tableName, boolean before, int type) {
        // no-op
    }

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) {

        int promovente = toInt(newRow[PROMOVENTE], 0);
        int estatus = toInt(newRow[ESTATUS_EXPE], 0);

        // ===== Defaults generales =====
        if (newRow[AVISO_ORGANO_JURIS] == null)
            newRow[AVISO_ORGANO_JURIS] = 9;

        if (newRow[AVISO_AUTORIDAD_ADMIN] == null)
            newRow[AVISO_AUTORIDAD_ADMIN] = 9;

        if (newRow[FECHA_PRESENTACION] == null)
            newRow[FECHA_PRESENTACION] = DEFAULT_DATE;

        if (newRow[FECHA_ADMISION] == null)
            newRow[FECHA_ADMISION] = DEFAULT_DATE;

        // ===== Promovente =====
        if (newRow[PROMOVENTE] == null)
            newRow[PROMOVENTE] = 9;

        if (promovente == 5 && newRow[ESPECIFIQUE] == null)
            newRow[ESPECIFIQUE] = "No Especifico";

        // ===== Estatus expediente =====
        if (newRow[ESTATUS_EXPE] == null)
            newRow[ESTATUS_EXPE] = 9;

        if (estatus == 1 && newRow[FECHA_RESOLUCION] == null)
            newRow[FECHA_RESOLUCION] = DEFAULT_DATE;

        // ===== Normalización de fechas incorrectas =====
        normalizeDate(newRow, FECHA_APERTURA_EXPEDIENTE);
        normalizeDate(newRow, FECHA_PRESENTACION);
        normalizeDate(newRow, FECHA_ADMISION);
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
