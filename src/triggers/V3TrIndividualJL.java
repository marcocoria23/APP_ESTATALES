package triggers;

import org.h2.api.Trigger;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;

public class V3TrIndividualJL implements Trigger {

    private static final LocalDate D_1899 = LocalDate.of(1899, 9, 9);
    private static final LocalDate D_1999 = LocalDate.of(1999, 9, 9);
    private static final String Sql_Error="INSERT INTO ERRORES_INSERT "
            + "(TABLA_DESTINO, CLAVE_ORGANO, EXPEDIENTE_CLAVE, ID, "
            + " SQLSTATE, ERRORCODE, MENSAJE, REGISTRO_RAW) "
            + "VALUES (?,?,?,?,?,?,?,?)";

    @Override
    public void init(Connection conn, String schemaName, String triggerName,
                     String tableName, boolean before, int type) {
        // no-op
    }

    // ===================== UTILIDADES =====================

    private Integer asInt(Object v) {
        if (v == null) return null;
        if (v instanceof Number) return ((Number) v).intValue();
        String s = v.toString().trim();
        if (s.isEmpty() || "null".equalsIgnoreCase(s)) return null;
        return Integer.valueOf(s);
    }
    
    private String asString(Object v) {
if (v == null) return null;
String s = v.toString().trim();
return (s.isEmpty() || "null".equalsIgnoreCase(s)) ? null : s;
}
    

      // OJO: Date.valueOf SOLO acepta yyyy-MM-dd
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

    private void setIfNull(Object[] row, int idx, Object value) {
        if (idx < row.length && row[idx] == null) {
            row[idx] = value;
        }
    }

    private void replace1999With1899(Object[] row, int idx) {
        if (idx >= row.length) return;
        Date d = asDate(row[idx]);
        if (d != null && d.equals(D_1999)) {
            row[idx] = D_1899;
        }
    }

    private Object safe(Object[] row, int idx) {
        return (row != null && idx >= 0 && idx < row.length)
                ? row[idx]
                : "<idx fuera de rango:" + idx + ">";
    }

    // ===================== TRIGGER =====================

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {

        try {
            // ===== Índices (0-based, AJUSTADOS AL CREATE TABLE) =====
            final int iCLAVE_ORGANO              = 1;
            final int iEXPEDIENTE_CLAVE          = 2;
            final int iFECHA_APERTURA            = 3;
            final int iTIPO_ASUNTO               = 4;
            final int iNAT_CONFLICTO             = 5;

            final int iCONTRATO_ESCRITO          = 6;
            final int iTIPO_CONTRATO             = 7;

            final int iRAMA                      = 8;
            final int iSECTOR                    = 9;
            final int iSUBSECTOR                 = 10;

            final int iSUBCONTRATACION           = 15;
            final int iINDOLE                    = 16;
            final int iPRESTACION_FP             = 17;
            final int iARRENDAMIENTO             = 18;
            final int iCAPACITACION              = 19;
            final int iANTIGUEDAD                = 20;
            final int iPRIMA_ANTIGUEDAD          = 21;
            final int iCONVENIO                  = 22;
            final int iDESIGNACION_FALLE         = 23;
            final int iDESIGNACION_DELITO        = 24;

            final int iINCOMPETENCIA             = 35;
            final int iTIPO_INCOMPETENCIA        = 36;
            final int iOTRO_ESP_INCOMP           = 37;

            final int iFECHA_PRES_DEMANDA        = 38;
            final int iESTATUS_DEMANDA           = 44;
            final int iFECHA_ADMI_DEMANDA        = 46;

            final int iESTATUS_EXPEDIENTE        = 55;
            final int iFECHA_ACTO_PROCESAL       = 56;
            final int iFASE_SOLI_EXPEDIENTE      = 57;

            final int iFORMA_SOL_TA              = 63;
            final int iOTRO_SOL_TA               = 64;
            final int iFECHA_RES_TA              = 65;
            final int iTIPO_SENT_TA              = 66;

            // ===== Defaults generales =====
            setIfNull(newRow, iTIPO_ASUNTO, 9);
            setIfNull(newRow, iNAT_CONFLICTO, 9);

            setIfNull(newRow, iCONTRATO_ESCRITO, 9);
            if (asInt(newRow[iCONTRATO_ESCRITO]) != null &&
                asInt(newRow[iCONTRATO_ESCRITO]) == 1) {
                setIfNull(newRow, iTIPO_CONTRATO, 9);
            }

            setIfNull(newRow, iRAMA, "No Identificado");
            setIfNull(newRow, iSECTOR, 99);
            setIfNull(newRow, iSUBSECTOR, 99);

            // ===== Incompetencia =====
            setIfNull(newRow, iINCOMPETENCIA, 9);
            Integer incompetencia = asInt(newRow[iINCOMPETENCIA]);

            if (incompetencia != null && incompetencia == 2) {
                setIfNull(newRow, iFECHA_PRES_DEMANDA, D_1899);
                setIfNull(newRow, iESTATUS_DEMANDA, 9);

                if (asInt(newRow[iESTATUS_DEMANDA]) != null &&
                    asInt(newRow[iESTATUS_DEMANDA]) == 1) {
                    setIfNull(newRow, iFECHA_ADMI_DEMANDA, D_1899);
                }
            }

            // ===== Estatus expediente =====
            Integer estExp = asInt(newRow[iESTATUS_EXPEDIENTE]);

            if (estExp != null && estExp == 2) {
                setIfNull(newRow, iFECHA_ACTO_PROCESAL, D_1899);
            }

            if (estExp != null && estExp == 1) {
                setIfNull(newRow, iFASE_SOLI_EXPEDIENTE, 99);
            }

            Integer fase = asInt(newRow[iFASE_SOLI_EXPEDIENTE]);

            if (fase != null && fase == 4) {
                setIfNull(newRow, iFORMA_SOL_TA, 9);
                setIfNull(newRow, iFECHA_RES_TA, D_1899);

                if (asInt(newRow[iFORMA_SOL_TA]) != null &&
                    asInt(newRow[iFORMA_SOL_TA]) == 1) {
                    setIfNull(newRow, iTIPO_SENT_TA, 9);
                }

                if (asInt(newRow[iFORMA_SOL_TA]) != null &&
                    asInt(newRow[iFORMA_SOL_TA]) == 5) {
                    setIfNull(newRow, iOTRO_SOL_TA, "No Especifico");
                }
            }

            // ===== Normalización de fechas =====
            replace1999With1899(newRow, iFECHA_APERTURA);
            replace1999With1899(newRow, iFECHA_PRES_DEMANDA);
            replace1999With1899(newRow, iFECHA_ADMI_DEMANDA);
            replace1999With1899(newRow, iFECHA_ACTO_PROCESAL);
            replace1999With1899(newRow, iFECHA_RES_TA);

           Integer tipoAsunto = asInt(newRow[iTIPO_ASUNTO]); 
       if (tipoAsunto != null && tipoAsunto == 2){
	String claveOrgano = asString(newRow[iCLAVE_ORGANO]);
        String expediente = asString(newRow[iEXPEDIENTE_CLAVE]);
          try ( PreparedStatement pe = conn.prepareStatement(Sql_Error)) {
            pe.setString(1, "V3_TR_INDIVIDUALJL");
            pe.setString(2, claveOrgano);
            pe.setString(3, expediente);
            pe.setString(4, "");
            pe.setString(5, "");
            pe.setInt(6, 999);
            pe.setString(7, "El campo Tipo_asunto solo puede tener el valor= 1.-Individual ");
            pe.setString(8, "");
            pe.executeUpdate();	
	} catch (SQLException ex) {
            // Si hasta la tabla de errores falla, al menos lo imprimimos
            System.err.println("❌ No se pudo guardar en ERRORES_INSERT: " + ex.getMessage());
        }
          
            }
            
       
      if (fase != null &&
(fase == 5 || fase == 6 || fase == 7 || fase == 8 || fase == 9)) {
	String claveOrgano = asString(newRow[iCLAVE_ORGANO]);
        String expediente = asString(newRow[iEXPEDIENTE_CLAVE]);
          try ( PreparedStatement pe = conn.prepareStatement(Sql_Error)) {
            pe.setString(1, "V3_TR_INDIVIDUALJL");
            pe.setString(2, claveOrgano);
            pe.setString(3, expediente);
            pe.setString(4, "");
            pe.setString(5, "");
            pe.setInt(6, 0);
            pe.setString(7, "El campo Fase_soli_expediente solo puede tener el valor= 1.-Audiencia preliminar,2.-Audiencia de juicio,3.-Tramitación por auto de depuración,4.-Tramitación sin audiencias");
            pe.setString(8, "");
            pe.executeUpdate();	
	} catch (SQLException ex) {
            // Si hasta la tabla de errores falla, al menos lo imprimimos
            System.err.println("❌ No se pudo guardar en ERRORES_INSERT: " + ex.getMessage());
        }
          
            }
            
        } catch (IllegalArgumentException e) {
            throw new SQLException(
                "Trigger V3TrIndividualJL IllegalArgumentException | "
                + "FECHA_APERTURA=" + safe(newRow, 3)
                + ", FECHA_PRES=" + safe(newRow, 38)
                + ", FECHA_ADMI=" + safe(newRow, 46)
                + ", newRow.length=" + newRow.length,
                e
            );
        } catch (RuntimeException e) {
            throw new SQLException(
                "Trigger V3TrIndividualJL RuntimeException | newRow.length=" + newRow.length,
                e
            );
        }
    }

    @Override
    public void close() {
        // no-op
    }

    @Override
    public void remove() {
        // no-op
    }
}
