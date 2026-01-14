package triggers;

import org.h2.api.Trigger;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;

public class V3TrProcesoHuelgaJL implements Trigger {

    // ====== AJUSTAR ÍNDICES SEGÚN CREATE TABLE ======
    private static final int COL_TIPO_ASUNTO = 1;
    private static final int COL_RAMA_INDUS_INVOLUCRAD = 2;
    private static final int COL_SECTOR_RAMA = 3;
    private static final int COL_SUBSECTOR_RAMA = 4;

    private static final int COL_FIRMA_CONTRATO = 5;
    private static final int COL_REVISION_CONTRATO = 6;
    private static final int COL_INCUMPLIM_CONTRATO = 7;
    private static final int COL_REVISION_SALARIO = 8;
    private static final int COL_REPARTO_UTILIDADES = 9;
    private static final int COL_APOYO_OTRA_HUELGA = 10;
    private static final int COL_DESEQUILIBRIO_FAC_PROD = 11;
    private static final int COL_OTRO_MOTIVO = 12;
    private static final int COL_ESPECIFIQUE_MOTIVO = 13;

    private static final int COL_INCOMPETENCIA = 14;
    private static final int COL_TIPO_INCOMPETENCIA = 15;
    private static final int COL_ESPECIFIQUE_INCOMP = 16;

    private static final int COL_FECHA_PRESENTA_PETIC = 17;
    private static final int COL_EMPLAZAMIENTO_HUELGA = 18;
    private static final int COL_FECHA_EMPLAZAMIENTO = 19;

    private static final int COL_PREHUELGA = 20;
    private static final int COL_AUDIENCIA_CONCILIACION = 21;
    private static final int COL_FECHA_AUDIENCIA = 22;

    private static final int COL_ESTALLAMIENTO_HUELGA = 23;
    private static final int COL_DECLARA_LICITUD_HUELGA = 24;
    private static final int COL_DECLARA_EXISTEN_HUELGA = 25;

    private static final int COL_ESTATUS_EXPEDIENTE = 26;
    private static final int COL_FECHA_ACTO_PROCESAL = 27;

    private static final int COL_FASE_SOLI_EXPEDIENTE = 28;
    private static final int COL_FORMA_SOLUCION_EMPLAZ = 29;
    private static final int COL_FECHA_RESOLU_EMPLAZ = 30;
    private static final int COL_ESPECIFI_FORMA_EMPLAZ = 31;

    private static final int COL_FORMA_SOLUCION_HUELGA = 32;
    private static final int COL_FECHA_RESOLU_HUELGA = 33;
    private static final int COL_FECHA_ESTALLAM_HUELGA = 34;
    private static final int COL_FECHA_LEVANT_HUELGA = 35;
    private static final int COL_TIPO_SENTENCIA = 36;
    private static final int COL_ESPECIFI_FORMA_HUELGA = 37;

    private static final int COL_FECHA_APERTURA_EXPEDIENTE = 38;

    // ====== FECHAS ======
    private static final Date FECHA_DEFAULT = Date.valueOf("1899-09-09");
    private static final Date FECHA_1999    = Date.valueOf("1999-09-09");

    @Override
    public void init(Connection conn, String schemaName, String triggerName,
                     String tableName, boolean before, int type) {}

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) {

        // ===== Defaults simples =====
        setIfNull(newRow, COL_TIPO_ASUNTO, 9);
        setIfNull(newRow, COL_RAMA_INDUS_INVOLUCRAD, "No Identificado");
        setIfNull(newRow, COL_SECTOR_RAMA, 99);
        setIfNull(newRow, COL_SUBSECTOR_RAMA, 99);

        setIfNull(newRow, COL_FIRMA_CONTRATO, 9);
        setIfNull(newRow, COL_REVISION_CONTRATO, 9);
        setIfNull(newRow, COL_INCUMPLIM_CONTRATO, 9);
        setIfNull(newRow, COL_REVISION_SALARIO, 9);
        setIfNull(newRow, COL_REPARTO_UTILIDADES, 9);
        setIfNull(newRow, COL_APOYO_OTRA_HUELGA, 9);
        setIfNull(newRow, COL_DESEQUILIBRIO_FAC_PROD, 9);
        setIfNull(newRow, COL_OTRO_MOTIVO, 9);

        if (is(newRow, COL_OTRO_MOTIVO, 1)) {
            setIfNull(newRow, COL_ESPECIFIQUE_MOTIVO, "No Especifico");
        }

        setIfNull(newRow, COL_INCOMPETENCIA, 9);

        // ===== Incompetencia = sí =====
        if (is(newRow, COL_INCOMPETENCIA, 1)) {
            setIfNull(newRow, COL_TIPO_INCOMPETENCIA, 9);

            if (is(newRow, COL_TIPO_INCOMPETENCIA, 4)) {
                setIfNull(newRow, COL_ESPECIFIQUE_INCOMP, "No identificado");
            }
        }

        // ===== Incompetencia = no =====
        if (is(newRow, COL_INCOMPETENCIA, 2)) {
            setIfNull(newRow, COL_FECHA_PRESENTA_PETIC, FECHA_DEFAULT);
            setIfNull(newRow, COL_EMPLAZAMIENTO_HUELGA, 9);

            if (is(newRow, COL_EMPLAZAMIENTO_HUELGA, 1)) {
                setIfNull(newRow, COL_FECHA_EMPLAZAMIENTO, FECHA_DEFAULT);
            }

            setIfNull(newRow, COL_PREHUELGA, 9);

            if (is(newRow, COL_PREHUELGA, 1)) {
                setIfNull(newRow, COL_AUDIENCIA_CONCILIACION, 9);

                if (is(newRow, COL_AUDIENCIA_CONCILIACION, 1)) {
                    setIfNull(newRow, COL_FECHA_AUDIENCIA, FECHA_DEFAULT);
                }
            }

            setIfNull(newRow, COL_ESTALLAMIENTO_HUELGA, 9);

            if (is(newRow, COL_ESTALLAMIENTO_HUELGA, 1)) {
                setIfNull(newRow, COL_DECLARA_LICITUD_HUELGA, 9);
                setIfNull(newRow, COL_DECLARA_EXISTEN_HUELGA, 9);
            }

            setIfNull(newRow, COL_ESTATUS_EXPEDIENTE, 9);
        }

        // ===== Estatus expediente =====
        if (is(newRow, COL_ESTATUS_EXPEDIENTE, 2)) {
            setIfNull(newRow, COL_FECHA_ACTO_PROCESAL, FECHA_DEFAULT);
        }

        if (is(newRow, COL_ESTATUS_EXPEDIENTE, 1)) {
            setIfNull(newRow, COL_FASE_SOLI_EXPEDIENTE, 99);
        }

        // ===== Soluciones =====
        if (is(newRow, COL_FASE_SOLI_EXPEDIENTE, 5) || is(newRow, COL_FASE_SOLI_EXPEDIENTE, 6)) {
            setIfNull(newRow, COL_FORMA_SOLUCION_EMPLAZ, 9);
            setIfNull(newRow, COL_FECHA_RESOLU_EMPLAZ, FECHA_DEFAULT);

            if (is(newRow, COL_FORMA_SOLUCION_EMPLAZ, 7)) {
                setIfNull(newRow, COL_ESPECIFI_FORMA_EMPLAZ, "No Especifico");
            }
        }

        if (is(newRow, COL_FASE_SOLI_EXPEDIENTE, 7)) {
            setIfNull(newRow, COL_FORMA_SOLUCION_HUELGA, 9);
            setIfNull(newRow, COL_FECHA_RESOLU_HUELGA, FECHA_DEFAULT);
            setIfNull(newRow, COL_FECHA_LEVANT_HUELGA, FECHA_DEFAULT);

            if (is(newRow, COL_ESTALLAMIENTO_HUELGA, 1)) {
                setIfNull(newRow, COL_FECHA_ESTALLAM_HUELGA, FECHA_DEFAULT);
            }
        }

        if (is(newRow, COL_FORMA_SOLUCION_HUELGA, 3)) {
            setIfNull(newRow, COL_TIPO_SENTENCIA, 9);
        }

        if (is(newRow, COL_FORMA_SOLUCION_HUELGA, 5)) {
            setIfNull(newRow, COL_ESPECIFI_FORMA_HUELGA, "No identificado");
        }

        // ===== Normalización fechas 1999 → 1899 =====
        normalizeDate(newRow, COL_FECHA_APERTURA_EXPEDIENTE);
        normalizeDate(newRow, COL_FECHA_PRESENTA_PETIC);
        normalizeDate(newRow, COL_FECHA_EMPLAZAMIENTO);
        normalizeDate(newRow, COL_FECHA_AUDIENCIA);
        normalizeDate(newRow, COL_FECHA_ACTO_PROCESAL);
        normalizeDate(newRow, COL_FECHA_RESOLU_EMPLAZ);
        normalizeDate(newRow, COL_FECHA_RESOLU_HUELGA);
        normalizeDate(newRow, COL_FECHA_ESTALLAM_HUELGA);
        normalizeDate(newRow, COL_FECHA_LEVANT_HUELGA);
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
            if (((Date) row[idx]).equals(FECHA_1999)) {
                row[idx] = FECHA_DEFAULT;
            }
        }
    }
}
