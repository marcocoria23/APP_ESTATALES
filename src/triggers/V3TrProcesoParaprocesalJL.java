package triggers;

import org.h2.api.Trigger;

import java.sql.Connection;
import java.sql.Date;

public class V3TrProcesoParaprocesalJL implements Trigger {

    // ===== Constants =====
    private static final Date DATE_1899 = Date.valueOf("1899-09-09");
    private static final Date DATE_1999 = Date.valueOf("1999-09-09");

    // ===== Column indexes (ADJUST TO YOUR TABLE) =====
    private static final int CLAVE_ORGANO = 1;
    private static final int FECHA_APERTURA_EXPEDIENTE = 2;

    private static final int RAMA_INVOLUCRAD = 5;
    private static final int SECTOR_RAMA = 6;
    private static final int SUBSECTOR_RAMA = 7;

    private static final int MOTIVO_SOLICITUD = 10;
    private static final int ESPECIFIQUE_MOTIVO = 11;

    private static final int INCOMPETENCIA = 12;
    private static final int TIPO_INCOMPETENCIA = 13;
    private static final int ESPECIFIQUE_INCOMP = 14;

    private static final int FECHA_PRESENTA_SOLI = 15;
    private static final int FECHA_ADMISION_SOLI = 16;

    private static final int PROMOVENTE = 17;
    private static final int ESPECIFIQUE_PROMOVENTE = 18;

    private static final int ESTATUS_EXPEDIENTE = 19;
    private static final int FECHA_CONCLUSION_EXPE = 20;

    @Override
    public void init(Connection conn, String schemaName, String triggerName,
                     String tableName, boolean before, int type) {
        // no-op
    }

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) {

        int motivoSolicitud = toInt(newRow[MOTIVO_SOLICITUD], 0);
        int incompetencia = toInt(newRow[INCOMPETENCIA], 0);
        int tipoIncompetencia = toInt(newRow[TIPO_INCOMPETENCIA], 0);
        int promovente = toInt(newRow[PROMOVENTE], 0);
        int estatusExpediente = toInt(newRow[ESTATUS_EXPEDIENTE], 0);

        // ===== Fix invalid initial date =====
        if ("310501".equals(newRow[CLAVE_ORGANO])
                && isDate(newRow[FECHA_APERTURA_EXPEDIENTE], Date.valueOf("1900-01-01"))) {
            newRow[FECHA_APERTURA_EXPEDIENTE] = DATE_1899;
        }

        // ===== Defaults =====
        if (newRow[RAMA_INVOLUCRAD] == null) newRow[RAMA_INVOLUCRAD] = "No Identificado";
        if (newRow[SECTOR_RAMA] == null) newRow[SECTOR_RAMA] = 99;
        if (newRow[SUBSECTOR_RAMA] == null) newRow[SUBSECTOR_RAMA] = 99;

        if (newRow[MOTIVO_SOLICITUD] == null) newRow[MOTIVO_SOLICITUD] = 99;

        // ===== Motivo solicitud =====
        if (motivoSolicitud == 10 && newRow[ESPECIFIQUE_MOTIVO] == null) {
            newRow[ESPECIFIQUE_MOTIVO] = "No Especifico";
        }

        // ===== Incompetencia =====
        if (newRow[INCOMPETENCIA] == null) newRow[INCOMPETENCIA] = 9;

        if (incompetencia == 1 && newRow[TIPO_INCOMPETENCIA] == null) {
            newRow[TIPO_INCOMPETENCIA] = 9;
        }

        if (incompetencia == 1 && tipoIncompetencia == 4
                && newRow[ESPECIFIQUE_INCOMP] == null) {
            newRow[ESPECIFIQUE_INCOMP] = "No Especifico";
        }

        // ===== Procede solicitud =====
        if (incompetencia == 2) {

            if (newRow[FECHA_PRESENTA_SOLI] == null)
                newRow[FECHA_PRESENTA_SOLI] = DATE_1899;

            if (newRow[FECHA_ADMISION_SOLI] == null)
                newRow[FECHA_ADMISION_SOLI] = DATE_1899;

            if (newRow[PROMOVENTE] == null)
                newRow[PROMOVENTE] = 9;

            if (promovente == 5 && newRow[ESPECIFIQUE_PROMOVENTE] == null)
                newRow[ESPECIFIQUE_PROMOVENTE] = "No Especifico";

            if (newRow[ESTATUS_EXPEDIENTE] == null)
                newRow[ESTATUS_EXPEDIENTE] = 9;

            if (estatusExpediente == 1 && newRow[FECHA_CONCLUSION_EXPE] == null)
                newRow[FECHA_CONCLUSION_EXPE] = DATE_1899;
        }

        // ===== Fix wrong sentinel dates =====
        fixDate(newRow, FECHA_APERTURA_EXPEDIENTE);
        fixDate(newRow, FECHA_PRESENTA_SOLI);
        fixDate(newRow, FECHA_ADMISION_SOLI);
        fixDate(newRow, FECHA_CONCLUSION_EXPE);
    }

    @Override public void close() {}
    @Override public void remove() {}

    // ===== Helpers =====

    private static int toInt(Object v, int def) {
        if (v == null) return def;
        if (v instanceof Number) return ((Number) v).intValue();
        try { return Integer.parseInt(v.toString()); }
        catch (Exception e) { return def; }
    }

    private static boolean isDate(Object v, Date d) {
        return (v instanceof Date) && d.equals(v);
    }

    private static void fixDate(Object[] row, int idx) {
        if (row[idx] instanceof Date && DATE_1999.equals(row[idx])) {
            row[idx] = DATE_1899;
        }
    }
}
