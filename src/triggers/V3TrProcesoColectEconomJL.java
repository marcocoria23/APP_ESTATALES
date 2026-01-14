package triggers;

import org.h2.api.Trigger;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;

public class V3TrProcesoColectEconomJL implements Trigger {

    // ====== COLUMN INDEXES (0-based, adjust to your table definition) ======
    private static final int IDX_TIPO_ASUNTO = 0;
    private static final int IDX_NAT_CONFLICTO = 1;
    private static final int IDX_RAMA_INVOLUCRAD = 2;
    private static final int IDX_SECTOR_RAMA = 3;
    private static final int IDX_SUBSECTOR_RAMA = 4;
    private static final int IDX_MODIF_CONDICIONES = 5;
    private static final int IDX_NUEVAS_CONDICIONES = 6;
    private static final int IDX_SUSPENSION_TEMPORAL = 7;
    private static final int IDX_TERMINACION_COLECTIVA = 8;
    private static final int IDX_OTRO_MOTIVO_ECONOM = 9;
    private static final int IDX_ESPECIFIQUE_ECONOM = 10;
    private static final int IDX_EXCESO_PRODUCCION = 11;
    private static final int IDX_INCOSTEABILIDAD = 12;
    private static final int IDX_FALTA_FONDOS = 13;
    private static final int IDX_INCOMPETENCIA = 14;
    private static final int IDX_TIPO_INCOMPETENCIA = 15;
    private static final int IDX_FECHA_PRES_DEMANDA = 16;
    private static final int IDX_CONSTANCIA_CONS_EXPEDIDA = 17;
    private static final int IDX_CONSTANCIA_CLAVE = 18;
    private static final int IDX_ASUN_EXCEP_CONCILIACION = 19;
    private static final int IDX_PREVE_DEMANDA = 20;
    private static final int IDX_DESAHOGO_PREV_DEMANDA = 21;
    private static final int IDX_ESTATUS_DEMANDA = 22;
    private static final int IDX_FECHA_ADMISION_DEMANDA = 23;
    private static final int IDX_AUDIENCIA_ECONOM = 24;
    private static final int IDX_FECHA_AUDIENCIA_ECONOM = 25;
    private static final int IDX_ESTATUS_EXPEDIENTE = 26;
    private static final int IDX_FASE_SOLI_EXPEDIENTE = 27;
    private static final int IDX_FORMA_SOLUCION = 28;
    private static final int IDX_FECHA_RESOLUCION = 29;
    private static final int IDX_TIPO_SENTENCIA = 30;
    private static final int IDX_AUMENTO_PERSONAL = 31;
    private static final int IDX_DISMINUCION_PERSONAL = 32;
    private static final int IDX_AUMENTO_JORNADALAB = 33;
    private static final int IDX_DISMINUCION_JORNADALAB = 34;
    private static final int IDX_AUMENTO_SEMANA = 35;
    private static final int IDX_DISMINUCION_SEMANA = 36;
    private static final int IDX_AUMENTO_SALARIOS = 37;
    private static final int IDX_DISMINUCION_SALARIOS = 38;
    private static final int IDX_OTRO_TIPO = 39;
    private static final int IDX_ESPECIFIQUE_TIPO = 40;
    private static final int IDX_FECHA_ACTO_PROCESAL = 41;
    private static final int IDX_FECHA_APERTURA_EXPEDIENTE = 42;

    private static final Date DATE_1899 = Date.valueOf("1899-09-09");
    private static final Date DATE_1999 = Date.valueOf("1999-09-09");

    @Override
    public void init(Connection conn, String schemaName, String triggerName,
                     String tableName, boolean before, int type) {}

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) {

        // ===== BASIC DEFAULTS =====
        setIfNull(newRow, IDX_TIPO_ASUNTO, 9);
        setIfNull(newRow, IDX_NAT_CONFLICTO, 9);
        setIfNull(newRow, IDX_RAMA_INVOLUCRAD, "No Identificado");
        setIfNull(newRow, IDX_SECTOR_RAMA, 99);
        setIfNull(newRow, IDX_SUBSECTOR_RAMA, 99);
        setIfNull(newRow, IDX_MODIF_CONDICIONES, 9);
        setIfNull(newRow, IDX_NUEVAS_CONDICIONES, 9);
        setIfNull(newRow, IDX_SUSPENSION_TEMPORAL, 9);
        setIfNull(newRow, IDX_TERMINACION_COLECTIVA, 9);
        setIfNull(newRow, IDX_OTRO_MOTIVO_ECONOM, 9);

        int otroMotivo = toInt(newRow[IDX_OTRO_MOTIVO_ECONOM]);
        int suspension = toInt(newRow[IDX_SUSPENSION_TEMPORAL]);
        int incompetencia = toInt(newRow[IDX_INCOMPETENCIA]);

        if (otroMotivo == 1 && newRow[IDX_ESPECIFIQUE_ECONOM] == null) {
            newRow[IDX_ESPECIFIQUE_ECONOM] = "No Especifico";
        }

        if (suspension == 1) {
            setIfNull(newRow, IDX_EXCESO_PRODUCCION, 9);
            setIfNull(newRow, IDX_INCOSTEABILIDAD, 9);
            setIfNull(newRow, IDX_FALTA_FONDOS, 9);
        }

        if (incompetencia == 1) {
            setIfNull(newRow, IDX_TIPO_INCOMPETENCIA, 9);
        }

        if (incompetencia == 2) {
            setIfNullDate(newRow, IDX_FECHA_PRES_DEMANDA);
            setIfNull(newRow, IDX_CONSTANCIA_CONS_EXPEDIDA, 9);

            int constancia = toInt(newRow[IDX_CONSTANCIA_CONS_EXPEDIDA]);
            if (constancia == 1 && newRow[IDX_CONSTANCIA_CLAVE] == null) {
                newRow[IDX_CONSTANCIA_CLAVE] = "No Identificado";
            }
            if (constancia == 2) {
                setIfNull(newRow, IDX_ASUN_EXCEP_CONCILIACION, 9);
            }

            setIfNull(newRow, IDX_PREVE_DEMANDA, 9);
            if (toInt(newRow[IDX_PREVE_DEMANDA]) == 1) {
                setIfNull(newRow, IDX_DESAHOGO_PREV_DEMANDA, 9);
            }

            setIfNull(newRow, IDX_ESTATUS_DEMANDA, 9);

            if (toInt(newRow[IDX_ESTATUS_DEMANDA]) == 1) {
                setIfNullDate(newRow, IDX_FECHA_ADMISION_DEMANDA);
                setIfNull(newRow, IDX_AUDIENCIA_ECONOM, 9);

                if (toInt(newRow[IDX_AUDIENCIA_ECONOM]) == 1) {
                    setIfNullDate(newRow, IDX_FECHA_AUDIENCIA_ECONOM);
                }

                setIfNull(newRow, IDX_ESTATUS_EXPEDIENTE, 9);

                if (toInt(newRow[IDX_ESTATUS_EXPEDIENTE]) == 1) {
                    setIfNull(newRow, IDX_FASE_SOLI_EXPEDIENTE, 99);

                    if (toInt(newRow[IDX_FASE_SOLI_EXPEDIENTE]) == 8) {
                        setIfNull(newRow, IDX_FORMA_SOLUCION, 9);
                        setIfNullDate(newRow, IDX_FECHA_RESOLUCION);

                        if (toInt(newRow[IDX_FORMA_SOLUCION]) == 1) {
                            setIfNull(newRow, IDX_TIPO_SENTENCIA, 9);
                            setIfNull(newRow, IDX_AUMENTO_PERSONAL, 9);
                            setIfNull(newRow, IDX_DISMINUCION_PERSONAL, 9);
                            setIfNull(newRow, IDX_AUMENTO_JORNADALAB, 9);
                            setIfNull(newRow, IDX_DISMINUCION_JORNADALAB, 9);
                            setIfNull(newRow, IDX_AUMENTO_SEMANA, 9);
                            setIfNull(newRow, IDX_DISMINUCION_SEMANA, 9);
                            setIfNull(newRow, IDX_AUMENTO_SALARIOS, 9);
                            setIfNull(newRow, IDX_DISMINUCION_SALARIOS, 9);
                            setIfNull(newRow, IDX_OTRO_TIPO, 9);

                            if (toInt(newRow[IDX_OTRO_TIPO]) == 1 &&
                                newRow[IDX_ESPECIFIQUE_TIPO] == null) {
                                newRow[IDX_ESPECIFIQUE_TIPO] = "No especifico";
                            }
                        }
                    }
                }

                if (toInt(newRow[IDX_ESTATUS_EXPEDIENTE]) == 2) {
                    setIfNullDate(newRow, IDX_FECHA_ACTO_PROCESAL);
                }
            }
        }

        // ===== FIX 1999 → 1899 =====
        fixDate(newRow, IDX_FECHA_APERTURA_EXPEDIENTE);
        fixDate(newRow, IDX_FECHA_PRES_DEMANDA);
        fixDate(newRow, IDX_FECHA_ADMISION_DEMANDA);
        fixDate(newRow, IDX_FECHA_AUDIENCIA_ECONOM);
        fixDate(newRow, IDX_FECHA_ACTO_PROCESAL);
        fixDate(newRow, IDX_FECHA_RESOLUCION);
    }

    @Override public void close() {}
    @Override public void remove() {}

    // ===== Helpers =====

    private static void setIfNull(Object[] row, int idx, int val) {
        if (row[idx] == null) row[idx] = BigDecimal.valueOf(val);
    }

    private static void setIfNull(Object[] row, int idx, String val) {
        if (row[idx] == null) row[idx] = val;
    }

    private static void setIfNullDate(Object[] row, int idx) {
        if (row[idx] == null) row[idx] = DATE_1899;
    }

    private static void fixDate(Object[] row, int idx) {
        if (row[idx] instanceof Date && row[idx].equals(DATE_1999)) {
            row[idx] = DATE_1899;
        }
    }

    private static int toInt(Object v) {
        if (v == null) return 0;
        if (v instanceof BigDecimal) return ((BigDecimal) v).intValue();
        try { return Integer.parseInt(v.toString()); }
        catch (Exception e) { return 0; }
    }
}
