package triggers;

import org.h2.api.Trigger;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;

public class V3TrColectivoJL implements Trigger {

    private static final LocalDate DATE_1899_09_09 = LocalDate.of(1899, 9, 9);
    private static final LocalDate DATE_1999_09_09 = LocalDate.of(1999, 9, 9);
    private static final String Sql_Error="INSERT INTO ERRORES_INSERT "
            + "(TABLA_DESTINO, CLAVE_ORGANO, EXPEDIENTE_CLAVE, ID, "
            + " SQLSTATE, ERRORCODE, MENSAJE, REGISTRO_RAW) "
            + "VALUES (?,?,?,?,?,?,?,?)";

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

    private void replace1999With1899(Object[] newRow, int idx) {
        Object v = newRow[idx];
        if (v == null) return;

        if (v instanceof Date) {
            if (DATE_1999_09_09.equals(v)) newRow[idx] = DATE_1899_09_09;
            return;
        }

        String s = v.toString().trim();
        if (s.equals("09/09/1999") || s.equals("1999-09-09")) {
            newRow[idx] = DATE_1899_09_09;
        }
    }
    
        private String asString(Object v) {
if (v == null) return null;
String s = v.toString().trim();
return (s.isEmpty() || "null".equalsIgnoreCase(s)) ? null : s;
}

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {
try {
        // ===== Índices (0-based) según el CREATE TABLE que pegaste =====
        // 0  NOMBRE_ORGANO_JURIS
        // 1  CLAVE_ORGANO
        // 2  EXPEDIENTE_CLAVE
        final int iCLAVE_ORGANO = 1;
        final int iEXPEDIENTE_CLAVE = 2;
        final int iFECHA_APERTURA_EXPEDIENTE = 3;

        final int iTIPO_ASUNTO = 4;
        final int iNAT_CONFLICTO = 5;

        final int iRAMA_INDUS_INVOLUCRAD = 6;
        final int iSECTOR_RAMA = 7;
        final int iSUBSECTOR_RAMA = 8;

        final int iDECLARACION_PERDIDA_MAY = 13;
        final int iSUSPENSION_TMP = 14;
        final int iTERMINACION_TRAB = 15;
        final int iCONTRATACION_COLECTIVA = 16;
        final int iOMISIONES_REGLAMENTO = 17;
        final int iREDUCCION_PERSONAL = 18;
        final int iVIOLA_DERECHOS = 19;
        final int iELECCION_SINDICALES = 20;
        final int iSANCION_SINDICALES = 21;
        final int iOTRO_CONFLICTO = 22;
        final int iOTRO_ESP_CONFLICTO = 23;

        final int iNO_IMPUTABLE_ST = 24;
        final int iINCAPACIDAD_FISICA_ST = 25;
        final int iFALTA_MATERIA_PRIM_ST = 26;
        final int iFALTA_MINISTRACION_ST = 27;

        final int iFUERZA_MAYOR_TC = 28;
        final int iINCAPACIDAD_FISICA_TC = 29;
        final int iQUIEBRA_LEGAL_TC = 30;
        final int iAGOTAMIENTO_MATERIA_TC = 31;

        final int iLIBERTAD_ASOCIACION = 32;
        final int iLIBERTAD_SINDICAL = 33;
        final int iDERECHO_COLECTIVA = 34;
        final int iOTRO_COLECTIVA = 35;
        final int iOTRO_ESP_COLECTIVA = 36;

        final int iINCOMPETENCIA = 37;
        final int iTIPO_INCOMPETENCIA = 38;

        final int iFECHA_PRES_DEMANDA = 40;

        final int iCONSTANCIA_CONS_EXPEDIDA = 41;
        final int iCONSTANCIA_CLAVE = 42;
        final int iASUN_EXCEP_CONCILIACION = 43;

        final int iPREVE_DEMANDA = 44;
        final int iDESAHOGO_PREV_DEMANDA = 45;

        final int iESTATUS_DEMANDA = 46;
        final int iFECHA_ADMI_DEMANDA = 47;

        final int iAUTO_DEPURACION = 50;
        final int iFECHA_DEPURACION = 51;

        final int iAUDIENCIA_JUICIO = 52;
        final int iFECHA_AUDIENCIA_JUICIO = 53;

        final int iESTATUS_EXPEDIENTE = 54;
        final int iFECHA_ACTO_PROCESAL = 55;

        final int iFASE_SOLI_EXPEDIENTE = 56;

        final int iFORMA_SOLUCION_AD = 57;
        final int iOTRO_ESP_SOLUCION_AD = 58;
        final int iFECHA_DICTO_RESOLUCION_AD = 59;
        final int iTIPO_SENTENCIA_AD = 60;

        final int iFORMA_SOLUCION_AJ = 62;
        final int iOTRO_ESP_SOLUCION_AJ = 63;
        final int iFECHA_RESOLUCION_AJ = 64;
        final int iTIPO_SENTENCIA_AJ = 65;

        // ===== Lógica del trigger =====

        // if :NEW.TIPO_ASUNTO IS NULL THEN 2
        setIfNull(newRow, iTIPO_ASUNTO, 2);

        // if :NEW.NAT_CONFLICTO IS NULL THEN 9
        setIfNull(newRow, iNAT_CONFLICTO, 9);

        // if :NEW.RAMA_INDUS_INVOLUCRAD IS NULL THEN 'No Identificado'
        setIfNull(newRow, iRAMA_INDUS_INVOLUCRAD, "No Identificado");

        // sector/subsector null => 99
        setIfNull(newRow, iSECTOR_RAMA, 99);
        setIfNull(newRow, iSUBSECTOR_RAMA, 99);

        // varios NOT NULL defaults => 9
        setIfNull(newRow, iDECLARACION_PERDIDA_MAY, 9);
        setIfNull(newRow, iSUSPENSION_TMP, 9);
        setIfNull(newRow, iTERMINACION_TRAB, 9);
        setIfNull(newRow, iCONTRATACION_COLECTIVA, 9);
        setIfNull(newRow, iOMISIONES_REGLAMENTO, 9);
        setIfNull(newRow, iREDUCCION_PERSONAL, 9);
        setIfNull(newRow, iVIOLA_DERECHOS, 9);
        setIfNull(newRow, iELECCION_SINDICALES, 9);
        setIfNull(newRow, iSANCION_SINDICALES, 9);
        setIfNull(newRow, iOTRO_CONFLICTO, 9);

        Integer otroConflicto = asInt(newRow[iOTRO_CONFLICTO]);
        if (otroConflicto != null && otroConflicto == 1) {
            setIfNull(newRow, iOTRO_ESP_CONFLICTO, "No Especifico");
        }

        Integer suspension = asInt(newRow[iSUSPENSION_TMP]);
        if (suspension != null && suspension == 1) {
            setIfNull(newRow, iNO_IMPUTABLE_ST, 9);
            setIfNull(newRow, iINCAPACIDAD_FISICA_ST, 9);
            setIfNull(newRow, iFALTA_MATERIA_PRIM_ST, 9);
            setIfNull(newRow, iFALTA_MINISTRACION_ST, 9);
        }

        Integer terminacion = asInt(newRow[iTERMINACION_TRAB]);
        if (terminacion != null && terminacion == 1) {
            setIfNull(newRow, iFUERZA_MAYOR_TC, 9);
            setIfNull(newRow, iINCAPACIDAD_FISICA_TC, 9);
            setIfNull(newRow, iQUIEBRA_LEGAL_TC, 9);
            setIfNull(newRow, iAGOTAMIENTO_MATERIA_TC, 9);
        }

        Integer viola = asInt(newRow[iVIOLA_DERECHOS]);
        if (viola != null && viola == 1) {

            // OJO: lo dejo igual que tu PL/SQL (aunque parezca typo)
            if (newRow[iLIBERTAD_ASOCIACION] == null) {
                newRow[iLIBERTAD_SINDICAL] = 9;
            }

            setIfNull(newRow, iLIBERTAD_SINDICAL, 9);
            setIfNull(newRow, iDERECHO_COLECTIVA, 9);
            setIfNull(newRow, iOTRO_COLECTIVA, 9);

            Integer otroColectiva = asInt(newRow[iOTRO_COLECTIVA]);
            if (otroColectiva != null && otroColectiva == 1) {
                setIfNull(newRow, iOTRO_ESP_COLECTIVA, "No Especifico");
            }
        }

        Integer inc = asInt(newRow[iINCOMPETENCIA]);

        // En tu trigger NO pones "if INCOMPETENCIA is null => 9" (ya es NOT NULL en tabla),
        // pero por si llega null en insert incompleto:
        setIfNull(newRow, iINCOMPETENCIA, 9);
        inc = asInt(newRow[iINCOMPETENCIA]);

        if (inc != null && inc == 1) {
            setIfNull(newRow, iTIPO_INCOMPETENCIA, 9);
        }

        if (inc != null && inc == 2) {

            setIfNull(newRow, iFECHA_PRES_DEMANDA, DATE_1899_09_09);
            setIfNull(newRow, iCONSTANCIA_CONS_EXPEDIDA, 9);

            Integer constExp = asInt(newRow[iCONSTANCIA_CONS_EXPEDIDA]);
            if (constExp != null && constExp == 1) {
                setIfNull(newRow, iCONSTANCIA_CLAVE, "No identidicada");
            }
            if (constExp != null && constExp == 2) {
                setIfNull(newRow, iASUN_EXCEP_CONCILIACION, 9);
            }

            setIfNull(newRow, iPREVE_DEMANDA, 9);
            Integer preve = asInt(newRow[iPREVE_DEMANDA]);
            if (preve != null && preve == 1) {
                setIfNull(newRow, iDESAHOGO_PREV_DEMANDA, 9);
            }

            setIfNull(newRow, iESTATUS_DEMANDA, 9);

            Integer estDem = asInt(newRow[iESTATUS_DEMANDA]);
            if (estDem != null && estDem == 1) {

                setIfNull(newRow, iFECHA_ADMI_DEMANDA, DATE_1899_09_09);

                setIfNull(newRow, iAUTO_DEPURACION, 9);
                Integer autoDep = asInt(newRow[iAUTO_DEPURACION]);
                if (autoDep != null && autoDep == 1) {
                    setIfNull(newRow, iFECHA_DEPURACION, DATE_1899_09_09);
                }

                setIfNull(newRow, iAUDIENCIA_JUICIO, 9);
                Integer audJ = asInt(newRow[iAUDIENCIA_JUICIO]);
                if (audJ != null && audJ == 1) {
                    setIfNull(newRow, iFECHA_AUDIENCIA_JUICIO, DATE_1899_09_09);
                }

                setIfNull(newRow, iESTATUS_EXPEDIENTE, 9);

                Integer estExp = asInt(newRow[iESTATUS_EXPEDIENTE]);
                if (estExp != null && estExp == 1) {
                    setIfNull(newRow, iFASE_SOLI_EXPEDIENTE, 99);

                    Integer fase = asInt(newRow[iFASE_SOLI_EXPEDIENTE]);

                    // fase=3 => AD
                    if (fase != null && fase == 3) {
                        setIfNull(newRow, iFORMA_SOLUCION_AD, 9);
                        Integer formaAD = asInt(newRow[iFORMA_SOLUCION_AD]);

                        if (formaAD != null && formaAD == 1) {
                            setIfNull(newRow, iFECHA_DICTO_RESOLUCION_AD, DATE_1899_09_09);
                            setIfNull(newRow, iTIPO_SENTENCIA_AD, 9);
                        }

                        if (formaAD != null && (formaAD == 2 || formaAD == 3 || formaAD == 4)) {
                            setIfNull(newRow, iFECHA_DICTO_RESOLUCION_AD, DATE_1899_09_09);
                        }

                        if (formaAD != null && formaAD == 5) {
                            setIfNull(newRow, iOTRO_ESP_SOLUCION_AD, "No Especifico");
                            setIfNull(newRow, iFECHA_DICTO_RESOLUCION_AD, DATE_1899_09_09);
                        }
                    }

                    // fase=2 => AJ
                    if (fase != null && fase == 2) {
                        setIfNull(newRow, iFORMA_SOLUCION_AJ, 9);
                        Integer formaAJ = asInt(newRow[iFORMA_SOLUCION_AJ]);

                        if (formaAJ != null && (formaAJ == 1 || formaAJ == 2 || formaAJ == 3 || formaAJ == 4)) {
                            setIfNull(newRow, iFECHA_RESOLUCION_AJ, DATE_1899_09_09);
                        }

                        if (formaAJ != null && formaAJ == 5) {
                            setIfNull(newRow, iOTRO_ESP_SOLUCION_AJ, "No Especifico");
                            setIfNull(newRow, iFECHA_RESOLUCION_AJ, DATE_1899_09_09);
                        }

                        if (formaAJ != null && formaAJ == 1) {
                            setIfNull(newRow, iTIPO_SENTENCIA_AJ, 9);
                        }
                    }
                }
            }
        }

        // ===== Reglas extra: 1999-09-09 => 1899-09-09 =====
        replace1999With1899(newRow, iFECHA_APERTURA_EXPEDIENTE);
        replace1999With1899(newRow, iFECHA_PRES_DEMANDA);
        replace1999With1899(newRow, iFECHA_ADMI_DEMANDA);
        replace1999With1899(newRow, iFECHA_DEPURACION);
        replace1999With1899(newRow, iFECHA_AUDIENCIA_JUICIO);
        replace1999With1899(newRow, iFECHA_ACTO_PROCESAL);
        replace1999With1899(newRow, iFECHA_DICTO_RESOLUCION_AD);
        replace1999With1899(newRow, iFECHA_RESOLUCION_AJ);
        Integer tipoAsunto = asInt(newRow[iTIPO_ASUNTO]);
        if (tipoAsunto != null && tipoAsunto == 1) {
	String claveOrgano = asString(newRow[iCLAVE_ORGANO]);
        String expediente = asString(newRow[iEXPEDIENTE_CLAVE]);
          try ( PreparedStatement pe = conn.prepareStatement(Sql_Error)) {
            pe.setString(1, "V3_TR_COLECTIVOJL");
            pe.setString(2, claveOrgano);
            pe.setString(3, expediente);
            pe.setString(4, "");
            pe.setString(5, "");
            pe.setInt(6, 999);
            pe.setString(7, "El campo Tipo_asunto solo puede tener el valor= 2.-Colectivo ");
            pe.setString(8, "");
            pe.executeUpdate();	
	} catch (SQLException ex) {
            // Si hasta la tabla de errores falla, al menos lo imprimimos
            System.err.println("❌ No se pudo guardar en ERRORES_INSERT: " + ex.getMessage());
        }
          
            }
        Integer fase = asInt(newRow[iFASE_SOLI_EXPEDIENTE]);
    if (fase != null
            && (fase == 1 || fase == 4 || fase == 5 || fase == 6
            || fase == 7 || fase == 8 || fase == 9)) {
        String claveOrgano = asString(newRow[iCLAVE_ORGANO]);
        String expediente = asString(newRow[iEXPEDIENTE_CLAVE]);
        try ( PreparedStatement pe = conn.prepareStatement(Sql_Error)) {
            pe.setString(1, "V3_TR_COLECTIVOJL");
            pe.setString(2, claveOrgano);
            pe.setString(3, expediente);
            pe.setString(4, "");
            pe.setString(5, "");
            pe.setInt(6, 0);
            pe.setString(7, "El campo Fase_soli_expediente solo puede tener el valor=2.-Audiencia de juicio,3.-Tramitación por auto de depuración");
            pe.setString(8, "");
            pe.executeUpdate();
        } catch (SQLException ex) {
            // Si hasta la tabla de errores falla, al menos lo imprimimos
            System.err.println("❌ No se pudo guardar en ERRORES_INSERT: " + ex.getMessage());
        }

    }
        
   } catch (Exception e) {
            System.out.println("EXCEPCION en trigger: " + e.getClass().getName() + " - " + e.getMessage());
            e.printStackTrace(System.out); // <-- AQUI veras la linea exacta
            throw e; // <-- importante: no te comas el error
        }

}

    @Override public void close() { }
    @Override public void remove() { }
}
