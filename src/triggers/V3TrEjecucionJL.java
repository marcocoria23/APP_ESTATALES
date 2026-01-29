package triggers;

import org.h2.api.Trigger;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.time.LocalDate;

public class V3TrEjecucionJL implements Trigger {
    private static final LocalDate D_1899 = LocalDate.of(1899, 9, 9);
    private static final LocalDate D_1999 = LocalDate.of(1999, 9, 9);
    
    @Override
    public void init(Connection conn, String schemaName, String triggerName,
                     String tableName, boolean before, int type) {
        // no-op
    }

    private Integer asInt(Object v) {
        if (v == null) return null;
        if (v instanceof Number) return ((Number) v).intValue();
        String s = v.toString().trim();
        if (s.isEmpty()) return null;
        return Integer.valueOf(s);
    }

    private void setIfNull(Object[] newRow, int idx, Object value) {
        if (newRow[idx] == null) newRow[idx] = value;
    }

        private Date asDate(Object v) {
    if (v == null) return null;


    // Si ya viene como java.sql.Date, úsalo tal cual (pero ojo: si el origen ya viene mal, aquí no lo arreglas)
    if (v instanceof Date) return (Date) v;


    // Si viene java.util.Date (incluye Timestamp), conviértelo a LocalDate para evitar TZ en el formateo final
    if (v instanceof java.util.Date) {
        java.util.Date ud = (java.util.Date) v;
        // Convertir a LocalDate en zona UTC para evitar que cambie el día entre PCs
        LocalDate ld = ud.toInstant().atZone(java.time.ZoneOffset.UTC).toLocalDate();
        return Date.valueOf(ld);
    }


    String s = v.toString().trim();
    if (s.isEmpty() || s.equalsIgnoreCase("null")) return null;


    LocalDate ld;


    // dd/MM/yyyy (CSV)
    if (s.matches("\\d{2}/\\d{2}/\\d{4}")) {
        if (s.equals("09/09/1999")) ld = D_1999;
        else if (s.equals("09/09/1899")) ld = D_1899;
        else {
            int dd = Integer.parseInt(s.substring(0, 2));
            int mm = Integer.parseInt(s.substring(3, 5));
            int yyyy = Integer.parseInt(s.substring(6, 10));
            ld = LocalDate.of(yyyy, mm, dd);
        }
        return Date.valueOf(ld);
    }


    // yyyy-MM-dd HH:mm:ss (o yyyy-MM-ddTHH:mm:ss) -> solo fecha
    if (s.length() >= 10 && s.charAt(4) == '-' && s.charAt(7) == '-') {
        ld = LocalDate.parse(s.substring(0, 10)); // yyyy-MM-dd
        return Date.valueOf(ld);
    }


    // yyyy-MM-dd
    ld = LocalDate.parse(s);
    return Date.valueOf(ld);
}

    private void replace1999With1899(Object[] newRow, int idxDate) {
        Date d = asDate(newRow[idxDate]);
        if (d != null && d.equals(D_1999)) newRow[idxDate] = D_1899;
    }

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {
try {
        // ===== Índices (0-based) según tu CREATE TABLE =====
        final int iFECHA_APERTURA_EXPEDIENTE = 3;
        final int iMOTIVO_SOLICITUD_EJ     = 4;
        final int iFECHA_PRESENTACION     = 5;
        final int iESTATUS_EXPE           = 6;
        final int iFECHA_CONCLUSION       = 7;
        final int iFASE_CONCLUSION        = 8;

        // ===== Defaults =====
        setIfNull(newRow, iMOTIVO_SOLICITUD_EJ, 9);
        setIfNull(newRow, iFECHA_PRESENTACION, D_1899);
        setIfNull(newRow, iESTATUS_EXPE, 9);

        // ===== Reglas por estatus =====
        Integer estatus = asInt(newRow[iESTATUS_EXPE]);
        if (estatus != null && estatus == 1) {
            setIfNull(newRow, iFECHA_CONCLUSION, D_1899);
            setIfNull(newRow, iFASE_CONCLUSION, 9);
        }

        // ===== Normalización 09/09/1999 -> 1899-09-09 =====
        replace1999With1899(newRow, iFECHA_APERTURA_EXPEDIENTE);
        replace1999With1899(newRow, iFECHA_PRESENTACION);
        replace1999With1899(newRow, iFECHA_CONCLUSION);
   
 } catch (Exception e) {
            System.out.println("EXCEPCION en trigger: " + e.getClass().getName() + " - " + e.getMessage());
            e.printStackTrace(System.out); // <-- AQUI veras la linea exacta
            throw e; // <-- importante: no te comas el error
        }

}

    @Override public void close() { }
    @Override public void remove() { }
}
