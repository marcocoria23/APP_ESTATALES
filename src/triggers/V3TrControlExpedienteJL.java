/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package triggers;

import org.h2.api.Trigger;
import java.math.BigDecimal;
import java.sql.Connection;

/**
 *
 * @author ANTONIO.CORIA
 */
public class V3TrControlExpedienteJL implements Trigger {

    /*
     * Índices (0-based) AJUSTA según el orden REAL de tu tabla
     *
     * 0  CIRCUNS_ORG_JUR
     * 1  OTRO_ESP_CIRCUNS
     * 2  JURISDICCION
     * 3  ORDINARIO
     * 4  ESPECIAL_INDIVI
     * 5  ESPECIAL_COLECT
     * 6  HUELGA
     * 7  COL_NATU_ECONOMICA
     * 8  PARAP_VOLUNTARIO
     * 9  TERCERIAS
     * 10 PREF_CREDITO
     * 11 EJECUCION
     * 12 LATITUD_ORG
     * 13 LONGITUD_ORG
     */

    private static final int IDX_CIRCUNS_ORG_JUR      = 0;
    private static final int IDX_OTRO_ESP_CIRCUNS     = 1;
    private static final int IDX_JURISDICCION         = 2;
    private static final int IDX_ORDINARIO            = 3;
    private static final int IDX_ESPECIAL_INDIVI      = 4;
    private static final int IDX_ESPECIAL_COLECT      = 5;
    private static final int IDX_HUELGA               = 6;
    private static final int IDX_COL_NATU_ECONOMICA   = 7;
    private static final int IDX_PARAP_VOLUNTARIO     = 8;
    private static final int IDX_TERCERIAS            = 9;
    private static final int IDX_PREF_CREDITO         = 10;
    private static final int IDX_EJECUCION             = 11;
    private static final int IDX_LATITUD_ORG          = 12;
    private static final int IDX_LONGITUD_ORG         = 13;

    @Override
    public void init(Connection conn, String schemaName, String triggerName,
                     String tableName, boolean before, int type) {
        // no-op
    }

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) {

        // ===== 1) CIRCUNS_ORG_JUR default = 9
        if (newRow[IDX_CIRCUNS_ORG_JUR] == null) {
            newRow[IDX_CIRCUNS_ORG_JUR] = BigDecimal.valueOf(9);
        }

        int circuns = toInt(newRow[IDX_CIRCUNS_ORG_JUR], 9);

        // ===== 2) Si CIRCUNS_ORG_JUR = 4 y OTRO_ESP_CIRCUNS es NULL
        if (circuns == 4 && newRow[IDX_OTRO_ESP_CIRCUNS] == null) {
            newRow[IDX_OTRO_ESP_CIRCUNS] = "No Especifico";
        }

        // ===== 3) JURISDICCION default = 9
        if (newRow[IDX_JURISDICCION] == null) {
            newRow[IDX_JURISDICCION] = BigDecimal.valueOf(9);
        }

        // ===== 4) Defaults numéricos = 0
        setZeroIfNull(newRow, IDX_ORDINARIO);
        setZeroIfNull(newRow, IDX_ESPECIAL_INDIVI);
        setZeroIfNull(newRow, IDX_ESPECIAL_COLECT);
        setZeroIfNull(newRow, IDX_HUELGA);
        setZeroIfNull(newRow, IDX_COL_NATU_ECONOMICA);
        setZeroIfNull(newRow, IDX_PARAP_VOLUNTARIO);
        setZeroIfNull(newRow, IDX_TERCERIAS);
        setZeroIfNull(newRow, IDX_PREF_CREDITO);
        setZeroIfNull(newRow, IDX_EJECUCION);

        // ===== 5) LATITUD_ORG default
        if (newRow[IDX_LATITUD_ORG] == null) {
            newRow[IDX_LATITUD_ORG] = "No identificado";
        }

        // ===== 6) LONGITUD_ORG default
        if (newRow[IDX_LONGITUD_ORG] == null) {
            newRow[IDX_LONGITUD_ORG] = "No identificado";
        }
    }

    @Override public void close() {}
    @Override public void remove() {}

    // ===== Helpers =====

    private static void setZeroIfNull(Object[] row, int idx) {
        if (row[idx] == null) {
            row[idx] = BigDecimal.ZERO;
        }
    }

    private static int toInt(Object v, int def) {
        if (v == null) return def;
        if (v instanceof Integer) return (Integer) v;
        if (v instanceof Long) return ((Long) v).intValue();
        if (v instanceof BigDecimal) return ((BigDecimal) v).intValue();
        try { return Integer.parseInt(v.toString()); } catch (Exception e) { return def; }
    }
}
