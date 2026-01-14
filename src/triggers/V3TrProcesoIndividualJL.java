package triggers;

import org.h2.api.Trigger;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;

public class V3TrProcesoIndividualJL implements Trigger {

    // ===== AJUSTA ÍNDICES SEGÚN CREATE TABLE =====
    private static final int COL_TIPO_ASUNTO = 1;
    private static final int COL_NAT_CONFLICTO = 2;
    private static final int COL_CONTRATO_ESCRITO = 3;
    private static final int COL_TIPO_CONTRATO = 4;

    private static final int COL_RAMA_INDUS_INVOLUCRADA = 5;
    private static final int COL_SECTOR_RAMA = 6;
    private static final int COL_SUBSECTOR_RAMA = 7;

    private static final int COL_SUBCONTRATACION = 8;
    private static final int COL_INDOLE_TRABAJO = 9;
    private static final int COL_PRESTACION_FP = 10;
    private static final int COL_ARRENDAM_TRAB = 11;
    private static final int COL_CAPACITACION = 12;
    private static final int COL_ANTIGUEDAD = 13;
    private static final int COL_PRIMA_ANTIGUEDAD = 14;
    private static final int COL_CONVENIO_TRAB = 15;
    private static final int COL_DESIGNACION_TRAB_FALLE = 16;
    private static final int COL_DESIGNACION_TRAB_ACT_DELIC = 17;
    private static final int COL_TERMINACION_LAB = 18;
    private static final int COL_RECUPERACION_CARGA = 19;
    private static final int COL_GASTOS_TRASLADOS = 20;
    private static final int COL_INDEMNIZACION = 21;
    private static final int COL_PAGO_INDEMNIZACION = 22;
    private static final int COL_DESACUERDO_MEDICOS = 23;
    private static final int COL_COBRO_PRESTACIONES = 24;
    private static final int COL_CONF_SEGURO_SOCIAL = 25;
    private static final int COL_OTRO_CONF = 26;
    private static final int COL_OTRO_ESP_CONF = 27;

    private static final int COL_INCOMPETENCIA = 28;
    private static final int COL_TIPO_INCOMPETENCIA = 29;
    private static final int COL_OTRO_ESP_INCOMP = 30;

    private static final int COL_FECHA_PRES_DEMANDA = 31;
    private static final int COL_CONSTANCIA_CONS_EXPEDIDA = 32;
    private static final int COL_CONSTANCIA_CLAVE = 33;
    private static final int COL_ASUN_EXCEP_CONCILIACION = 34;
    private static final int COL_PREVE_DEMANDA = 35;
    private static final int COL_DESAHOGO_PREV_DEMANDA = 36;
    private static final int COL_ESTATUS_DEMANDA = 37;
    private static final int COL_CAU_IMPI_ADMI_DEMANDA = 38;
    private static final int COL_FECHA_ADMI_DEMANDA = 39;
    private static final int COL_TRAMITACION_DEPURACION = 40;
    private static final int COL_FECHA_DEPURACION = 41;
    private static final int COL_AUDIENCIA_PRELIM = 42;
    private static final int COL_FECHA_AUDIENCIA_PRELIM = 43;
    private static final int COL_AUDIENCIA_JUICIO = 44;
    private static final int COL_FECHA_AUDIENCIA_JUICIO = 45;

    private static final int COL_ESTATUS_EXPEDIENTE = 46;
    private static final int COL_FECHA_ACTO_PROCESAL = 47;
    private static final int COL_FASE_SOLI_EXPEDIENTE = 48;

    private static final int COL_FORMA_SOLUCION_AD = 49;
    private static final int COL_FECHA_DICTO_RESOLUCION_AD = 50;
    private static final int COL_TIPO_SENTENCIA_AD = 51;
    private static final int COL_OTRO_ESP_SOLUCION_AD = 52;

    private static final int COL_FORMA_SOLUCION_TA = 53;
    private static final int COL_FECHA_RESOLUCION_TA = 54;
    private static final int COL_TIPO_SENTENCIA_TA = 55;
    private static final int COL_OTRO_ESP_SOLUCION_TA = 56;

    private static final int COL_FORMA_SOLUCION_AP = 57;
    private static final int COL_FECHA_DICTO_RESOLUCION_AP = 58;
    private static final int COL_OTRO_ESP_SOLUCION_AP = 59;

    private static final int COL_FORMA_SOLUCION_AJ = 60;
    private static final int COL_FECHA_DICTO_RESOLUCION_AJ = 61;
    private static final int COL_TIPO_SENTENCIA_AJ = 62;
    private static final int COL_OTRO_ESP_SOLUCION_AJ = 63;

    private static final int COL_CLAVE_ORGANO = 64;
    private static final int COL_EXPEDIENTE_CLAVE = 65;
    private static final int COL_FECHA_APERTURA_EXPEDIENTE = 66;

    // ===== FECHAS =====
    private static final Date FECHA_DEFAULT = Date.valueOf("1899-09-09");
    private static final Date FECHA_1999 = Date.valueOf("1999-09-09");

    @Override
    public void init(Connection conn, String schemaName, String triggerName,
                     String tableName, boolean before, int type) {}

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) {

        // ===== Defaults básicos =====
        setIfNull(newRow, COL_TIPO_ASUNTO, 9);
        setIfNull(newRow, COL_NAT_CONFLICTO, 9);
        setIfNull(newRow, COL_CONTRATO_ESCRITO, 9);

        if (is(newRow, COL_CONTRATO_ESCRITO, 1)) {
            setIfNull(newRow, COL_TIPO_CONTRATO, 9);
        }

        setIfNull(newRow, COL_RAMA_INDUS_INVOLUCRADA, "No Identificado");
        setIfNull(newRow, COL_SECTOR_RAMA, 99);
        setIfNull(newRow, COL_SUBSECTOR_RAMA, 99);

        // ===== Respuestas simples =====
        int[] simples = {
            COL_SUBCONTRATACION, COL_INDOLE_TRABAJO, COL_PRESTACION_FP,
            COL_ARRENDAM_TRAB, COL_CAPACITACION, COL_ANTIGUEDAD,
            COL_PRIMA_ANTIGUEDAD, COL_CONVENIO_TRAB,
            COL_DESIGNACION_TRAB_FALLE, COL_DESIGNACION_TRAB_ACT_DELIC,
            COL_TERMINACION_LAB, COL_RECUPERACION_CARGA,
            COL_GASTOS_TRASLADOS, COL_INDEMNIZACION,
            COL_PAGO_INDEMNIZACION, COL_DESACUERDO_MEDICOS,
            COL_COBRO_PRESTACIONES, COL_CONF_SEGURO_SOCIAL,
            COL_OTRO_CONF
        };

        for (int c : simples) {
            setIfNull(newRow, c, 9);
        }

        if (is(newRow, COL_OTRO_CONF, 1)) {
            setIfNull(newRow, COL_OTRO_ESP_CONF, "No Especifico");
        }

        // ===== Incompetencia =====
        setIfNull(newRow, COL_INCOMPETENCIA, 9);

        if (is(newRow, COL_INCOMPETENCIA, 1)) {
            setIfNull(newRow, COL_TIPO_INCOMPETENCIA, 9);
            if (is(newRow, COL_TIPO_INCOMPETENCIA, 4)) {
                setIfNull(newRow, COL_OTRO_ESP_INCOMP, "No Especifico");
            }
        }

        if (is(newRow, COL_INCOMPETENCIA, 2)) {
            setIfNull(newRow, COL_FECHA_PRES_DEMANDA, FECHA_DEFAULT);
            setIfNull(newRow, COL_CONSTANCIA_CONS_EXPEDIDA, 9);
            setIfNull(newRow, COL_PREVE_DEMANDA, 9);
            setIfNull(newRow, COL_ESTATUS_DEMANDA, 9);
        }

        // ===== Normalización fechas =====
        for (int c : new int[]{
                COL_FECHA_APERTURA_EXPEDIENTE, COL_FECHA_PRES_DEMANDA,
                COL_FECHA_ADMI_DEMANDA, COL_FECHA_DEPURACION,
                COL_FECHA_AUDIENCIA_PRELIM, COL_FECHA_AUDIENCIA_JUICIO,
                COL_FECHA_ACTO_PROCESAL, COL_FECHA_DICTO_RESOLUCION_AD,
                COL_FECHA_RESOLUCION_TA, COL_FECHA_DICTO_RESOLUCION_AP,
                COL_FECHA_DICTO_RESOLUCION_AJ
        }) {
            normalizeDate(newRow, c);
        }
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
        if (row[idx] instanceof Date && row[idx].equals(FECHA_1999)) {
            row[idx] = FECHA_DEFAULT;
        }
    }
}
