/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package triggers;

import org.h2.api.Trigger;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author ANTONIO.CORIA
 */
public class V3TrAudienciasJL implements Trigger {

    // Índices (0-based) según tu CREATE TABLE:
    // 0 NOMBRE_ORGANO_JURIS
    // 1 CLAVE_ORGANO
    // 2 EXPEDIENTE_CLAVE
    // 3 TIPO_PROCED
    // 4 ID_AUDIENCIA
    // 5 ORDINARIO_TA
    // 6 ESPECIAL_INDIVI_TA
    // 7 ESPECIAL_COLECT_TA
    // 8 HUELGA_TA
    // 9 COL_NATU_ECONOMICA_TA
    // 10 ESP_OTRO_AUDIENCIA
    // 11 FECHA_AUDIEN_CELEBRADA
    // 12 INICIO
    // 13 CONCLU
    // 14 COMENTARIOS
    // 15 PERIODO

    private static final int IDX_TIPO_PROCED = 3;
    private static final int IDX_ORDINARIO_TA = 5;
    private static final int IDX_ESPECIAL_INDIVI_TA = 6;
    private static final int IDX_ESPECIAL_COLECT_TA = 7;
    private static final int IDX_HUELGA_TA = 8;
    private static final int IDX_COL_NATU_ECONOMICA_TA = 9;
    private static final int IDX_FECHA = 11;
    private static final int IDX_INICIO = 12;
    private static final int IDX_CONCLU = 13;

    private static final Date DATE_1899_09_09 = Date.valueOf("1899-09-09");
    private static final Date DATE_1999_09_09 = Date.valueOf("1999-09-09");

    @Override
    public void init(Connection conn, String schemaName, String triggerName,
                     String tableName, boolean before, int type) {
        // no-op
    }

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) {
try {
        // ===== 1) Si TIPO_PROCED es NULL => 9  (igual que Oracle)
        if (newRow[IDX_TIPO_PROCED] == null) {
            newRow[IDX_TIPO_PROCED] = BigDecimal.valueOf(9);
        }

        int tipo = toInt(newRow[IDX_TIPO_PROCED], 9);

        // ===== 3) Defaults dependientes de tipo
        if (tipo == 1 && newRow[IDX_ORDINARIO_TA] == null) {
            newRow[IDX_ORDINARIO_TA] = BigDecimal.valueOf(9);
        }
        if (tipo == 2 && newRow[IDX_ESPECIAL_INDIVI_TA] == null) {
            newRow[IDX_ESPECIAL_INDIVI_TA] = BigDecimal.valueOf(9);
        }
        if (tipo == 3 && newRow[IDX_ESPECIAL_COLECT_TA] == null) {
            newRow[IDX_ESPECIAL_COLECT_TA] = BigDecimal.valueOf(9);
        }
        if (tipo == 4 && newRow[IDX_HUELGA_TA] == null) {
            newRow[IDX_HUELGA_TA] = BigDecimal.valueOf(9);
        }
        if (tipo == 6 && newRow[IDX_COL_NATU_ECONOMICA_TA] == null) {
            newRow[IDX_COL_NATU_ECONOMICA_TA] = BigDecimal.valueOf(9);
        }

        // ===== 4) FECHA default
        if (newRow[IDX_FECHA] == null) {
            newRow[IDX_FECHA] = DATE_1899_09_09;
        }

        // ===== 5) INICIO default + normalización
        String inicio = toStr(newRow[IDX_INICIO]);
        if (inicio == null || "00:00".equals(inicio)) {
            newRow[IDX_INICIO] = "99:99";
        }

        // ===== 6) CONCLU default + normalización
        String conclu = toStr(newRow[IDX_CONCLU]);
        if (conclu == null || "00:00".equals(conclu)) {
            newRow[IDX_CONCLU] = "99:99";
        }

        // ===== 7) Si FECHA es 1999-09-09 => 1899-09-09
        Date fecha = toDate(newRow[IDX_FECHA]);
        if (fecha != null && fecha.equals(DATE_1999_09_09)) {
            newRow[IDX_FECHA] = DATE_1899_09_09;
        }
     } catch (Exception e) {
            System.out.println("EXCEPCION en trigger: " + e.getClass().getName() + " - " + e.getMessage());
            e.printStackTrace(System.out); // <-- AQUI veras la linea exacta
            throw e; // <-- importante: no te comas el error
        }

}



    @Override public void close() {}
    @Override public void remove() {}

    // ===== Helpers =====

  /*  private static int countTipoProced(Connection conn, int tipo) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT COUNT(*) FROM V3_TC_AUD_TIPO_PROCEJL WHERE ID = ?")) {
            ps.setInt(1, tipo);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }*/

    private static int toInt(Object v, int def) {
        if (v == null) return def;
        if (v instanceof Integer) return (Integer) v;
        if (v instanceof Long) return ((Long) v).intValue();
        if (v instanceof BigDecimal) return ((BigDecimal) v).intValue();
        try { return Integer.parseInt(v.toString()); } catch (Exception e) { return def; }
    }

    private static String toStr(Object v) {
        if (v == null) return null;
        String s = v.toString();
        return s.isEmpty() ? null : s;
    }

    private static Date toDate(Object v) {
        if (v == null) return null;
        if (v instanceof Date) return (Date) v;
        // Si viniera como 'YYYY-MM-DD'
        try { return Date.valueOf(v.toString()); } catch (Exception e) { return null; }
    }
}
