package triggers;

import org.h2.api.Trigger;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Date;
import java.time.LocalDate;

public class V3TrPreferenciaCreditoJL implements Trigger {

   private static final LocalDate D1899 = LocalDate.of(1899, 9, 9);
    private static final LocalDate D1999 = LocalDate.of(1999, 9, 9);
    
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

    private void replaceDate1999To1899(Object[] newRow, int idx) {
        Object v = newRow[idx];
        if (v == null) return;

        // si viene como java.sql.Date
        if (v instanceof Date) {
            if (((Date) v).equals(D1999)) newRow[idx] = D1899;
            return;
        }

        // si viene como String tipo '09/09/1999' o '1999-09-09'
        String s = v.toString().trim();
        if (s.equals("09/09/1999") || s.equals("1999-09-09")) {
            newRow[idx] = D1899;
        }
    }

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {
try {
        // ===== Índices (0-based) según tu CREATE TABLE =====
        final int iNOMBRE_ORGANO_JURIS   = 0;
        final int iCLAVE_ORGANO          = 1;
        final int iEXPEDIENTE_CLAVE      = 2;

        final int iFECHA_APERTURA_EXP    = 3;
        final int iAVISO_ORGANO_JURIS    = 4;
        final int iAVISO_AUTORIDAD_ADMIN = 5;

        final int iFECHA_PRESENTACION    = 6;
        final int iFECHA_ADMISION        = 7;

        final int iPROMOVENTE            = 8;
        final int iESPECIFIQUE           = 9;

        final int iESTATUS_EXPE          = 10;
        final int iFECHA_RESOLUCION      = 11;

        // 12 comentarios

        // ===== Defaults =====
        setIfNull(newRow, iAVISO_ORGANO_JURIS, 9);
        setIfNull(newRow, iAVISO_AUTORIDAD_ADMIN, 9);

        setIfNull(newRow, iFECHA_PRESENTACION, D1899);
        setIfNull(newRow, iFECHA_ADMISION, D1899);

        setIfNull(newRow, iPROMOVENTE, 9);

        Integer promovente = asInt(newRow[iPROMOVENTE]);
        if (promovente != null && promovente == 5) {
            setIfNull(newRow, iESPECIFIQUE, "No Especifico");
        }

        setIfNull(newRow, iESTATUS_EXPE, 9);

        Integer estatus = asInt(newRow[iESTATUS_EXPE]);
        if (estatus != null && estatus == 1) {
            setIfNull(newRow, iFECHA_RESOLUCION, D1899);
        }

        // ===== Reemplazos 09/09/1999 -> 09/09/1899 =====
        replaceDate1999To1899(newRow, iFECHA_APERTURA_EXP);
        replaceDate1999To1899(newRow, iFECHA_PRESENTACION);
        replaceDate1999To1899(newRow, iFECHA_ADMISION);
        replaceDate1999To1899(newRow, iFECHA_RESOLUCION);
 } catch (Exception e) {
            System.out.println("EXCEPCION en trigger: " + e.getClass().getName() + " - " + e.getMessage());
            e.printStackTrace(System.out); // <-- AQUI veras la linea exacta
            throw e; // <-- importante: no te comas el error
        }

}

    @Override public void close() { }
    @Override public void remove() { }
}
