package triggers;

import org.h2.api.Trigger;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;

public class V3TrColectivoJL implements Trigger {

    private static final Date DATE_1899_09_09 = Date.valueOf("1899-09-09");
    private static final Date DATE_1999_09_09 = Date.valueOf("1999-09-09");

    @Override
    public void init(Connection conn, String schemaName, String triggerName,
                     String tableName, boolean before, int type) {
    }

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) {

        // === Defaults ===
        setIfNull(newRow, col("TIPO_ASUNTO"), 2);
        setIfNull(newRow, col("NAT_CONFLICTO"), 9);
        setIfNull(newRow, col("RAMA_INDUS_INVOLUCRAD"), "No Identificado");
        setIfNull(newRow, col("SECTOR_RAMA"), 99);
        setIfNull(newRow, col("SUBSECTOR_RAMA"), 99);

        setIfNull(newRow, col("DECLARACION_PERDIDA_MAY"), 9);
        setIfNull(newRow, col("SUSPENSION_TMP"), 9);
        setIfNull(newRow, col("TERMINACION_TRAB"), 9);
        setIfNull(newRow, col("CONTRATACION_COLECTIVA"), 9);
        setIfNull(newRow, col("OMISIONES_REGLAMENTO"), 9);
        setIfNull(newRow, col("REDUCCION_PERSONAL"), 9);
        setIfNull(newRow, col("VIOLA_DERECHOS"), 9);
        setIfNull(newRow, col("ELECCION_SINDICALES"), 9);
        setIfNull(newRow, col("SANCION_SINDICALES"), 9);
        setIfNull(newRow, col("OTRO_CONFLICTO"), 9);

        // === Conditional logic ===

        if (is(newRow, "OTRO_CONFLICTO", 1)) {
            setIfNull(newRow, col("OTRO_ESP_CONFLICTO"), "No Especifico");
        }

        if (is(newRow, "SUSPENSION_TMP", 1)) {
            setIfNull(newRow, col("NO_IMPUTABLE_ST"), 9);
            setIfNull(newRow, col("INCAPACIDAD_FISICA_ST"), 9);
            setIfNull(newRow, col("FALTA_MATERIA_PRIM_ST"), 9);
            setIfNull(newRow, col("FALTA_MINISTRACION_ST"), 9);
        }

        if (is(newRow, "TERMINACION_TRAB", 1)) {
            setIfNull(newRow, col("FUERZA_MAYOR_TC"), 9);
            setIfNull(newRow, col("INCAPACIDAD_FISICA_TC"), 9);
            setIfNull(newRow, col("QUIEBRA_LEGAL_TC"), 9);
            setIfNull(newRow, col("AGOTAMIENTO_MATERIA_TC"), 9);
        }

        if (is(newRow, "VIOLA_DERECHOS", 1)) {
            setIfNull(newRow, col("LIBERTAD_SINDICAL"), 9);
            setIfNull(newRow, col("DERECHO_COLECTIVA"), 9);
            setIfNull(newRow, col("OTRO_COLECTIVA"), 9);

            if (is(newRow, "OTRO_COLECTIVA", 1)) {
                setIfNull(newRow, col("OTRO_ESP_COLECTIVA"), "No Especifico");
            }
        }

        // === Incompetencia branch ===
        if (is(newRow, "INCOMPETENCIA", 1)) {
            setIfNull(newRow, col("TIPO_INCOMPETENCIA"), 9);
        }

        if (is(newRow, "INCOMPETENCIA", 2)) {

            setIfNull(newRow, col("FECHA_PRES_DEMANDA"), DATE_1899_09_09);
            setIfNull(newRow, col("CONSTANCIA_CONS_EXPEDIDA"), 9);
            setIfNull(newRow, col("PREVE_DEMANDA"), 9);
            setIfNull(newRow, col("ESTATUS_DEMANDA"), 9);

            if (is(newRow, "CONSTANCIA_CONS_EXPEDIDA", 1)) {
                setIfNull(newRow, col("CONSTANCIA_CLAVE"), "No identidicada");
            }

            if (is(newRow, "CONSTANCIA_CONS_EXPEDIDA", 2)) {
                setIfNull(newRow, col("ASUN_EXCEP_CONCILIACION"), 9);
            }

            if (is(newRow, "PREVE_DEMANDA", 1)) {
                setIfNull(newRow, col("DESAHOGO_PREV_DEMANDA"), 9);
            }

            if (is(newRow, "ESTATUS_DEMANDA", 1)) {
                setIfNull(newRow, col("FECHA_ADMI_DEMANDA"), DATE_1899_09_09);
                setIfNull(newRow, col("AUTO_DEPURACION"), 9);
                setIfNull(newRow, col("AUDIENCIA_JUICIO"), 9);
                setIfNull(newRow, col("ESTATUS_EXPEDIENTE"), 9);
            }
        }

        // === Fix wrong dates (1999 → 1899) ===
        fixDate(newRow, "FECHA_APERTURA_EXPEDIENTE");
        fixDate(newRow, "FECHA_PRES_DEMANDA");
        fixDate(newRow, "FECHA_ADMI_DEMANDA");
        fixDate(newRow, "FECHA_DEPURACION");
        fixDate(newRow, "FECHA_AUDIENCIA_JUICIO");
        fixDate(newRow, "FECHA_ACTO_PROCESAL");
        fixDate(newRow, "FECHA_DICTO_RESOLUCION_AD");
        fixDate(newRow, "FECHA_RESOLUCION_AJ");
    }

    @Override public void close() {}
    @Override public void remove() {}

    // ===== Helpers =====

    private static int col(String name) {
        // 🔴 IMPORTANT: replace with real column index
        throw new UnsupportedOperationException(
            "Replace col(\"" + name + "\") with actual column index");
    }

    private static boolean is(Object[] row, String col, int val) {
        Object v = row[col(col)];
        if (v == null) return false;
        if (v instanceof BigDecimal) return ((BigDecimal) v).intValue() == val;
        return Integer.parseInt(v.toString()) == val;
    }

    private static void setIfNull(Object[] row, int idx, Object value) {
        if (row[idx] == null) {
            row[idx] = value instanceof Integer
                    ? BigDecimal.valueOf((Integer) value)
                    : value;
        }
    }

    private static void fixDate(Object[] row, String col) {
        int idx = col(col);
        if (row[idx] instanceof Date &&
            DATE_1999_09_09.equals(row[idx])) {
            row[idx] = DATE_1899_09_09;
        }
    }
}
