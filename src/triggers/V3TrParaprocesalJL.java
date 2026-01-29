package triggers;

import org.h2.api.Trigger;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.time.LocalDate;

public class V3TrParaprocesalJL implements Trigger {

    
    private static final LocalDate D_1899 = LocalDate.of(1899, 9, 9);
    private static final LocalDate D_1999 = LocalDate.of(1999, 9, 9);
    private static final LocalDate D_1900 = LocalDate.of(1900, 1, 1);
   
    
    
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
    private void setIfNull(Object[] newRow, int idx, Object value) {
        if (newRow[idx] == null) newRow[idx] = value;
    }

    private void replace1999With1899(Object[] newRow, int idxDate) {
        Date d = asDate(newRow[idxDate]);
        if (d != null && d.equals(D_1999)) newRow[idxDate] = D_1899;
    }

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {
try {
        // ===== Índices (0-based) según tu CREATE TABLE =====
        final int iNOMBRE_ORGANO_JURIS        = 0;
        final int iCLAVE_ORGANO               = 1;
        final int iEXPEDIENTE_CLAVE           = 2;
        final int iFECHA_APERTURA_EXPEDIENTE  = 3;

        final int iRAMA_INVOLUCRAD            = 4;
        final int iSECTOR_RAMA                = 5;
        final int iSUBSECTOR_RAMA             = 6;

        final int iMOTIVO_SOLICITUD           = 7;
        final int iESPECIFIQUE_MOTIVO         = 8;

        final int iINCOMPETENCIA              = 9;
        final int iTIPO_INCOMPETENCIA         = 10;
        final int iESPECIFIQUE_INCOMP         = 11;

        final int iFECHA_PRESENTA_SOLI        = 12;
        final int iFECHA_ADMISION_SOLI        = 13;

        final int iPROMOVENTE                 = 14;
        final int iESPECIFIQUE_PROMOVENTE     = 15;

        final int iESTATUS_EXPEDIENTE         = 16;
        final int iFECHA_CONCLUSION_EXPE      = 17;

        // ===== Reglas Oracle =====

        // IF CLAVE_ORGANO='310501' AND FECHA_APERTURA_EXPEDIENTE='01/01/1900' THEN -> 1899-09-09
        String claveOrgano = (newRow[iCLAVE_ORGANO] == null) ? null : newRow[iCLAVE_ORGANO].toString();
        Date fApertura = asDate(newRow[iFECHA_APERTURA_EXPEDIENTE]);
        if ("310501".equals(claveOrgano) && fApertura != null && fApertura.equals(D_1900)) {
            newRow[iFECHA_APERTURA_EXPEDIENTE] = D_1899;
        }

        if (newRow[iRAMA_INVOLUCRAD] == null) newRow[iRAMA_INVOLUCRAD] = "No Identificado";
        setIfNull(newRow, iSECTOR_RAMA, 99);
        setIfNull(newRow, iSUBSECTOR_RAMA, 99);

        setIfNull(newRow, iMOTIVO_SOLICITUD, 99);
        Integer motivo = asInt(newRow[iMOTIVO_SOLICITUD]);
        if (motivo != null && motivo == 10) {
            setIfNull(newRow, iESPECIFIQUE_MOTIVO, "No Especifico");
        }

        setIfNull(newRow, iINCOMPETENCIA, 9);
        Integer incompetencia = asInt(newRow[iINCOMPETENCIA]);

        // incompetencia = 1
        if (incompetencia != null && incompetencia == 1) {
            setIfNull(newRow, iTIPO_INCOMPETENCIA, 9);

            Integer tipoIncomp = asInt(newRow[iTIPO_INCOMPETENCIA]);
            if (tipoIncomp != null && tipoIncomp == 4) {
                setIfNull(newRow, iESPECIFIQUE_INCOMP, "No Especifico");
            }
        }

        // incompetencia = 2
        if (incompetencia != null && incompetencia == 2) {

            setIfNull(newRow, iFECHA_PRESENTA_SOLI, D_1899);
            setIfNull(newRow, iFECHA_ADMISION_SOLI, D_1899);

            setIfNull(newRow, iPROMOVENTE, 9);
            Integer promovente = asInt(newRow[iPROMOVENTE]);
            if (promovente != null && promovente == 5) {
                setIfNull(newRow, iESPECIFIQUE_PROMOVENTE, "No Especifico");
            }

            setIfNull(newRow, iESTATUS_EXPEDIENTE, 9);
            Integer estExp = asInt(newRow[iESTATUS_EXPEDIENTE]);

            if (estExp != null && estExp == 1) {
                setIfNull(newRow, iFECHA_CONCLUSION_EXPE, D_1899);
            }
        }

        // ===== Normalización 1999-09-09 -> 1899-09-09 (como tu Oracle) =====
        replace1999With1899(newRow, iFECHA_APERTURA_EXPEDIENTE);
        replace1999With1899(newRow, iFECHA_PRESENTA_SOLI);
        replace1999With1899(newRow, iFECHA_ADMISION_SOLI);
        replace1999With1899(newRow, iFECHA_CONCLUSION_EXPE);
} catch (Exception e) {
            System.out.println("EXCEPCION en trigger: " + e.getClass().getName() + " - " + e.getMessage());
            e.printStackTrace(System.out); // <-- AQUI veras la linea exacta
            throw e; // <-- importante: no te comas el error
        }

}
    @Override public void close() { }
    @Override public void remove() { }
}
