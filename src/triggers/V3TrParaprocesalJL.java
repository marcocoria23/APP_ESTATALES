package triggers;

import org.h2.api.Trigger;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;

public class V3TrParaprocesalJL implements Trigger {

    private static final Date D_1899 = Date.valueOf("1899-09-09");
    private static final Date D_1999 = Date.valueOf("1999-09-09");
    private static final Date D_1900 = Date.valueOf("1900-01-01");

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
        if (v instanceof Date) return (Date) v;
        if (v instanceof java.util.Date) return new Date(((java.util.Date) v).getTime());

        String s = v.toString().trim();
        if (s.isEmpty() || s.equalsIgnoreCase("null")) return null;

        // dd/MM/yyyy (por si llega desde CSV)
        if (s.matches("\\d{2}/\\d{2}/\\d{4}")) {
            if (s.equals("09/09/1999")) return D_1999;
            if (s.equals("09/09/1899")) return D_1899;
            String yyyy = s.substring(6, 10);
            String mm = s.substring(3, 5);
            String dd = s.substring(0, 2);
            return Date.valueOf(yyyy + "-" + mm + "-" + dd);
        }

        // yyyy-MM-dd HH:mm:ss -> corta a fecha
        if (s.length() >= 10 && s.charAt(4) == '-' && s.charAt(7) == '-') {
            return Date.valueOf(s.substring(0, 10));
        }

        return Date.valueOf(s); // yyyy-MM-dd
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
