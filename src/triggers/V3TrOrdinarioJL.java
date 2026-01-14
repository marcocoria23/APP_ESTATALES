package triggers;

import org.h2.api.Trigger;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;

public class V3TrOrdinarioJL implements Trigger {

    // ===== Common constants =====
    private static final BigDecimal N9 = BigDecimal.valueOf(9);
    private static final BigDecimal N99 = BigDecimal.valueOf(99);
    private static final Date DATE_1899 = Date.valueOf("1899-09-09");
    private static final Date DATE_1999 = Date.valueOf("1999-09-09");

    // ===== Column indexes (ADJUST TO YOUR TABLE) =====
    private static final int TIPO_ASUNTO = 2;
    private static final int CONTRATO_ESCRITO = 5;
    private static final int PAGO_PRESTACIONES = 6;
    private static final int CIRCUNS_MOTIVO_CONFL = 7;
    private static final int INCOMPETENCIA = 15;

    private static final int NAT_CONFLICTO = 20;
    private static final int RAMA_INDUS_INVOLUCRADA = 21;
    private static final int SECTOR_RAMA = 22;
    private static final int SUBSECTOR_RAMA = 23;

    // Dates
    private static final int FECHA_APERTURA_EXPEDIENTE = 40;
    private static final int FECHA_PRES_DEMANDA = 41;
    private static final int FECHA_ADMI_DEMANDA = 42;
    private static final int FECHA_AUDIENCIA_PRELIM = 43;
    private static final int FECHA_AUDIENCIA_JUICIO = 44;
    private static final int FECHA_ACTO_PROCESAL = 45;
    private static final int FECHA_DICTO_RESOLUCIONFE = 46;
    private static final int FECHA_DICTO_RESOLUCIONAP = 47;
    private static final int FECHA_RESOLUCIONAJ = 48;

    @Override
    public void init(Connection conn, String schemaName, String triggerName,
                     String tableName, boolean before, int type) {
        // no-op
    }

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) {

        int tipoAsunto = toInt(newRow[TIPO_ASUNTO], 0);
        int contrato = toInt(newRow[CONTRATO_ESCRITO], 0);
        int pagoPrest = toInt(newRow[PAGO_PRESTACIONES], 0);
        int circuns = toInt(newRow[CIRCUNS_MOTIVO_CONFL], 0);
        int incompetencia = toInt(newRow[INCOMPETENCIA], 0);

        // ===== Defaults =====
        if (newRow[NAT_CONFLICTO] == null) newRow[NAT_CONFLICTO] = N9;
        if (newRow[RAMA_INDUS_INVOLUCRADA] == null) newRow[RAMA_INDUS_INVOLUCRADA] = "No Identificado";
        if (newRow[SECTOR_RAMA] == null) newRow[SECTOR_RAMA] = N99;
        if (newRow[SUBSECTOR_RAMA] == null) newRow[SUBSECTOR_RAMA] = N99;

        // ===== Tipo asunto = 1 =====
        if (tipoAsunto == 1) {

            setIfNull(newRow, CONTRATO_ESCRITO, N9);

            if (contrato == 1) {
                setIfNull(newRow, idx("TIPO_CONTRATO"), N9);
            }

            setIfNull(newRow, idx("SUBCONTRATACION"), N9);
            setIfNull(newRow, idx("DESPIDO"), N9);
            setIfNull(newRow, idx("RESCISION_RL"), N9);
            setIfNull(newRow, idx("TERMINACION_RESCISION_RL"), N9);
            setIfNull(newRow, idx("VIOLACION_CONTRATO"), N9);
            setIfNull(newRow, idx("RIESGO_TRABAJO"), N9);
            setIfNull(newRow, idx("REVISION_CONTRATO"), N9);
            setIfNull(newRow, idx("PART_UTILIDADES"), N9);
            setIfNull(newRow, idx("OTRO_MOTIV_CONFLICTO"), N9);
            setIfNull(newRow, CIRCUNS_MOTIVO_CONFL, N9);

            // Discriminación
            if (circuns == 1) {
                setIfNull(newRow, idx("DETERM_EMPLEO_EMBARAZO"), N9);
                setIfNull(newRow, idx("DETERM_EMPLEO_EDAD"), N9);
                setIfNull(newRow, idx("DETERM_EMPLEO_GENERO"), N9);
                setIfNull(newRow, idx("DETERM_EMPLEO_ORIEN_SEX"), N9);
                setIfNull(newRow, idx("DETERM_EMPLEO_DISCAPACIDAD"), N9);
                setIfNull(newRow, idx("DETERM_EMPLEO_SOCIAL"), N9);
                setIfNull(newRow, idx("DETERM_EMPLEO_ORIGEN"), N9);
                setIfNull(newRow, idx("DETERM_EMPLEO_RELIGION"), N9);
                setIfNull(newRow, idx("DETERM_EMPLEO_MIGRA"), N9);
                setIfNull(newRow, idx("OTRO_DISCRIMINACION"), N9);
            }

            // Prestaciones
            setIfNull(newRow, PAGO_PRESTACIONES, N9);
            if (pagoPrest == 1) {
                setIfNull(newRow, idx("AGUINALDO"), N9);
                setIfNull(newRow, idx("VACACIONES"), N9);
                setIfNull(newRow, idx("PRIMA_VACACIONAL"), N9);
                setIfNull(newRow, idx("PRIMA_ANTIGUEDAD"), N9);
                setIfNull(newRow, idx("OTRO_TIPO_PREST"), N9);
            }
        }

        // ===== Incompetencia =====
        if (newRow[INCOMPETENCIA] == null) newRow[INCOMPETENCIA] = N9;

        if (incompetencia == 2) {
            setIfNull(newRow, idx("FECHA_PRES_DEMANDA"), DATE_1899);
            setIfNull(newRow, idx("CONSTANCIA_CONS_EXPEDIDA"), N9);
            setIfNull(newRow, idx("PREVE_DEMANDA"), N9);
            setIfNull(newRow, idx("ESTATUS_DEMANDA"), N9);
        }

        // ===== Fix wrong dates =====
        fixDate(newRow, FECHA_APERTURA_EXPEDIENTE);
        fixDate(newRow, FECHA_PRES_DEMANDA);
        fixDate(newRow, FECHA_ADMI_DEMANDA);
        fixDate(newRow, FECHA_AUDIENCIA_PRELIM);
        fixDate(newRow, FECHA_AUDIENCIA_JUICIO);
        fixDate(newRow, FECHA_ACTO_PROCESAL);
        fixDate(newRow, FECHA_DICTO_RESOLUCIONFE);
        fixDate(newRow, FECHA_DICTO_RESOLUCIONAP);
        fixDate(newRow, FECHA_RESOLUCIONAJ);
    }

    @Override public void close() {}
    @Override public void remove() {}

    // ===== Helpers =====

    private static void setIfNull(Object[] row, int idx, Object value) {
        if (row[idx] == null) row[idx] = value;
    }

    private static void fixDate(Object[] row, int idx) {
        if (row[idx] instanceof Date && DATE_1999.equals(row[idx])) {
            row[idx] = DATE_1899;
        }
    }

    private static int toInt(Object v, int def) {
        if (v == null) return def;
        if (v instanceof Number) return ((Number) v).intValue();
        try { return Integer.parseInt(v.toString()); }
        catch (Exception e) { return def; }
    }

    // Placeholder mapping (replace with real indexes)
    private static int idx(String columnName) {
        throw new IllegalStateException("Map column index for: " + columnName);
    }
}
