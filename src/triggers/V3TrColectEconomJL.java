package triggers;

import org.h2.api.Trigger;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class V3TrColectEconomJL implements Trigger {

    private static final Date D_1899 = Date.valueOf("1899-09-09");
    private static final Date D_1999 = Date.valueOf("1999-09-09");
     private static final Date DATE_1899_09_09 = Date.valueOf("1899-09-09");
    private static final Date DATE_1999_09_09 = Date.valueOf("1999-09-09");
    private static final String Sql_Error="INSERT INTO ERRORES_INSERT "
            + "(TABLA_DESTINO, CLAVE_ORGANO, EXPEDIENTE_CLAVE, ID, "
            + " SQLSTATE, ERRORCODE, MENSAJE, REGISTRO_RAW) "
            + "VALUES (?,?,?,?,?,?,?,?)";

    @Override
    public void init(Connection conn, String schemaName, String triggerName,
                     String tableName, boolean before, int type) {
        // no-op
    }

    // ===== DEBUG HELPERS =====
    private void dbg(String msg) {
        System.out.println("[V3TrColectEconomJL] " + msg);
    }

    private Object get(Object[] row, int idx, String name) {
        if (row == null) throw new IllegalArgumentException("newRow es null");
        if (idx < 0 || idx >= row.length) {
            throw new IllegalArgumentException(
                "Indice fuera de rango: " + name + " idx=" + idx + " row.length=" + row.length
            );
        }
        return row[idx];
    }

    private void dumpCell(Object[] row, int idx, String name) {
        Object v = get(row, idx, name);
        //dbg(name + "[" + idx + "]=" + v + " (" + (v == null ? "null" : v.getClass().getName()) + ")");
    }

    private Integer asInt(Object v) {
        if (v == null) return null;
        if (v instanceof Number) return ((Number) v).intValue();
        String s = v.toString().trim();
        if (s.isEmpty() || s.equalsIgnoreCase("null")) return null;
        return Integer.valueOf(s); // <-- si v trae "Valor Cat No encontrado" truena aquí
    }

    private void setIfNullDbg(Object[] newRow, int idx, String name, Object value) {
        Object cur = get(newRow, idx, name); // valida rango
        if (cur == null) {
            dbg("SET " + name + "[" + idx + "] = " + value + " (" +
                (value == null ? "null" : value.getClass().getName()) + ")");
            newRow[idx] = value;
        }
    }

    private Date asDate(Object v) {
        if (v == null) return null;
        if (v instanceof Date) return (Date) v;
        if (v instanceof java.util.Date) return new Date(((java.util.Date) v).getTime());

        String s = v.toString().trim();
        if (s.isEmpty() || s.equalsIgnoreCase("null")) return null;

        // dd/MM/yyyy (por si viene del CSV)
        if (s.matches("\\d{2}/\\d{2}/\\d{4}")) {
            if (s.equals("09/09/1999")) return D_1999;
            if (s.equals("09/09/1899")) return D_1899;
            String yyyy = s.substring(6, 10);
            String mm = s.substring(3, 5);
            String dd = s.substring(0, 2);
            return Date.valueOf(yyyy + "-" + mm + "-" + dd);
        }

        // yyyy-MM-dd HH:mm:ss -> solo fecha
        if (s.length() >= 10 && s.charAt(4) == '-' && s.charAt(7) == '-') {
            return Date.valueOf(s.substring(0, 10));
        }

        return Date.valueOf(s); // yyyy-MM-dd
    }
    
            private String asString(Object v) {
if (v == null) return null;
String s = v.toString().trim();
return (s.isEmpty() || "null".equalsIgnoreCase(s)) ? null : s;
}

    private void replace1999With1899Dbg(Object[] newRow, int idxDate, String name) {
        Object raw = get(newRow, idxDate, name);
        try {
            Date d = asDate(raw);
            if (d != null && d.equals(D_1999)) {
                dbg("NORMALIZA " + name + "[" + idxDate + "] " + raw + " -> " + D_1899);
                newRow[idxDate] = D_1899;
            }
        } catch (Exception e) {
            dbg("ERROR convirtiendo fecha en " + name + "[" + idxDate + "], valor=" + raw +
                " (" + (raw == null ? "null" : raw.getClass().getName()) + ")");
            throw e;
        }
    }

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {

         try {
            //dbg("fire() row.length=" + (newRow == null ? -1 : newRow.length));
        // ===== Índices (0-based) según tu CREATE TABLE =====
         final int iCLAVE_ORGANO = 1;
        final int iEXPEDIENTE_CLAVE = 2;
        final int iFECHA_APERTURA_EXPEDIENTE = 3;
        final int iTIPO_ASUNTO = 4;
        final int iNAT_CONFLICTO = 5;
        final int iRAMA_INVOLUCRAD = 6;
        final int iSECTOR_RAMA = 7;
        final int iSUBSECTOR_RAMA = 8;
        final int iMODIF_CONDICIONES = 13;
        final int iNUEVAS_CONDICIONES = 14;
        final int iSUSPENSION_TEMPORAL = 15;
        final int iTERMINACION_COLECTIVA = 16;
        final int iOTRO_MOTIVO_ECONOM = 17;
        final int iESPECIFIQUE_ECONOM = 18;
        final int iEXCESO_PRODUCCION = 19;
        final int iINCOSTEABILIDAD = 20;
        final int iFALTA_FONDOS = 21;
        final int iINCOMPETENCIA = 22;
        final int iTIPO_INCOMPETENCIA = 23;
        final int iESPECIFIQUE_INCOMP = 24;
        final int iFECHA_PRES_DEMANDA = 25;
        final int iCONSTANCIA_CONS_EXPEDIDA = 26;
        final int iCONSTANCIA_CLAVE = 27;
        final int iASUN_EXCEP_CONCILIACION = 28;
        final int iPREVE_DEMANDA = 29;
        final int iDESAHOGO_PREV_DEMANDA = 30;
        final int iESTATUS_DEMANDA = 31;
        final int iFECHA_ADMISION_DEMANDA = 32;
        final int iAUDIENCIA_ECONOM = 35;
        final int iFECHA_AUDIENCIA_ECONOM = 36;
        final int iESTATUS_EXPEDIENTE = 37;
        final int iFECHA_ACTO_PROCESAL = 38;
        final int iFASE_SOLI_EXPEDIENTE = 39;
        final int iFORMA_SOLUCION = 40;
        final int iESPECIFIQUE_FORMA = 41;
        final int iFECHA_RESOLUCION = 42;
        final int iTIPO_SENTENCIA = 43;
        final int iAUMENTO_PERSONAL = 44;
        final int iDISMINUCION_PERSONAL = 45;
        final int iAUMENTO_JORNADALAB = 46;
        final int iDISMINUCION_JORNADALAB = 47;
        final int iAUMENTO_SEMANA = 48;
        final int iDISMINUCION_SEMANA = 49;
        final int iAUMENTO_SALARIOS = 50;
        final int iDISMINUCION_SALARIOS = 51;
        final int iOTRO_TIPO = 52;
        final int iESPECIFIQUE_TIPO = 53;
        
            // Imprime algunas columnas clave al inicio (para ver tipos/valores)
            dumpCell(newRow, iSECTOR_RAMA, "SECTOR_RAMA");
            dumpCell(newRow, iSUBSECTOR_RAMA, "SUBSECTOR_RAMA");
            dumpCell(newRow, iTIPO_ASUNTO, "TIPO_ASUNTO");
            dumpCell(newRow, iINCOMPETENCIA, "INCOMPETENCIA");

           
            setIfNullDbg(newRow, iTIPO_ASUNTO, "TIPO_ASUNTO", 9);
            setIfNullDbg(newRow, iNAT_CONFLICTO, "NAT_CONFLICTO", 9);

            setIfNullDbg(newRow, iRAMA_INVOLUCRAD, "RAMA_INVOLUCRAD", "No Identificado");
            setIfNullDbg(newRow, iSECTOR_RAMA, "SECTOR_RAMA", 99);
            setIfNullDbg(newRow, iSUBSECTOR_RAMA, "SUBSECTOR_RAMA", 99);

            setIfNullDbg(newRow, iMODIF_CONDICIONES, "MODIF_CONDICIONES", 9);
            setIfNullDbg(newRow, iNUEVAS_CONDICIONES, "NUEVAS_CONDICIONES", 9);
            setIfNullDbg(newRow, iSUSPENSION_TEMPORAL, "SUSPENSION_TEMPORAL", 9);
            setIfNullDbg(newRow, iTERMINACION_COLECTIVA, "TERMINACION_COLECTIVA", 9);
            setIfNullDbg(newRow, iOTRO_MOTIVO_ECONOM, "OTRO_MOTIVO_ECONOM", 9);

           
            Integer otroMotivo = asInt(get(newRow, iOTRO_MOTIVO_ECONOM, "OTRO_MOTIVO_ECONOM"));
            if (otroMotivo != null && otroMotivo == 1) {
                setIfNullDbg(newRow, iESPECIFIQUE_ECONOM, "ESPECIFIQUE_ECONOM", "No Especifico");
            }

            Integer suspension = asInt(get(newRow, iSUSPENSION_TEMPORAL, "SUSPENSION_TEMPORAL"));
            if (suspension != null && suspension == 1) {
                setIfNullDbg(newRow, iEXCESO_PRODUCCION, "EXCESO_PRODUCCION", 9);
                setIfNullDbg(newRow, iINCOSTEABILIDAD, "INCOSTEABILIDAD", 9);
                setIfNullDbg(newRow, iFALTA_FONDOS, "FALTA_FONDOS", 9);
            }

            Integer incompetencia = asInt(get(newRow, iINCOMPETENCIA, "INCOMPETENCIA"));
            if (incompetencia != null && incompetencia == 1) {
                setIfNullDbg(newRow, iTIPO_INCOMPETENCIA, "TIPO_INCOMPETENCIA", 9);
            }

            if (incompetencia != null && incompetencia == 2) {
                setIfNullDbg(newRow, iFECHA_PRES_DEMANDA, "FECHA_PRES_DEMANDA", D_1899);
                setIfNullDbg(newRow, iCONSTANCIA_CONS_EXPEDIDA, "CONSTANCIA_CONS_EXPEDIDA", 9);

                Integer constExp = asInt(get(newRow, iCONSTANCIA_CONS_EXPEDIDA, "CONSTANCIA_CONS_EXPEDIDA"));
                if (constExp != null && constExp == 1) {
                    setIfNullDbg(newRow, iCONSTANCIA_CLAVE, "CONSTANCIA_CLAVE", "No Identificado");
                }
                if (constExp != null && constExp == 2) {
                    setIfNullDbg(newRow, iASUN_EXCEP_CONCILIACION, "ASUN_EXCEP_CONCILIACION", 9);
                }

                setIfNullDbg(newRow, iPREVE_DEMANDA, "PREVE_DEMANDA", 9);

                Integer preve = asInt(get(newRow, iPREVE_DEMANDA, "PREVE_DEMANDA"));
                if (preve != null && preve == 1) {
                    setIfNullDbg(newRow, iDESAHOGO_PREV_DEMANDA, "DESAHOGO_PREV_DEMANDA", 9);
                }

                setIfNullDbg(newRow, iESTATUS_DEMANDA, "ESTATUS_DEMANDA", 9);

                Integer estatusDem = asInt(get(newRow, iESTATUS_DEMANDA, "ESTATUS_DEMANDA"));
                if (estatusDem != null && estatusDem == 1) {
                    setIfNullDbg(newRow, iFECHA_ADMISION_DEMANDA, "FECHA_ADMISION_DEMANDA", D_1899);
                    setIfNullDbg(newRow, iAUDIENCIA_ECONOM, "AUDIENCIA_ECONOM", 9);

                    Integer audEconom = asInt(get(newRow, iAUDIENCIA_ECONOM, "AUDIENCIA_ECONOM"));
                    if (audEconom != null && audEconom == 1) {
                        setIfNullDbg(newRow, iFECHA_AUDIENCIA_ECONOM, "FECHA_AUDIENCIA_ECONOM", D_1899);
                    }

                    setIfNullDbg(newRow, iESTATUS_EXPEDIENTE, "ESTATUS_EXPEDIENTE", 9);

                    Integer estatusExp = asInt(get(newRow, iESTATUS_EXPEDIENTE, "ESTATUS_EXPEDIENTE"));
                    if (estatusExp != null && estatusExp == 1) {
                        setIfNullDbg(newRow, iFASE_SOLI_EXPEDIENTE, "FASE_SOLI_EXPEDIENTE", 99);

                        Integer fase = asInt(get(newRow, iFASE_SOLI_EXPEDIENTE, "FASE_SOLI_EXPEDIENTE"));
                        if (fase != null && fase == 8) {
                            setIfNullDbg(newRow, iFORMA_SOLUCION, "FORMA_SOLUCION", 9);
                            setIfNullDbg(newRow, iFECHA_RESOLUCION, "FECHA_RESOLUCION", D_1899);

                            Integer forma = asInt(get(newRow, iFORMA_SOLUCION, "FORMA_SOLUCION"));
                            if (forma != null && forma == 1) {
                                setIfNullDbg(newRow, iTIPO_SENTENCIA, "TIPO_SENTENCIA", 9);
                                setIfNullDbg(newRow, iAUMENTO_PERSONAL, "AUMENTO_PERSONAL", 9);
                                setIfNullDbg(newRow, iDISMINUCION_PERSONAL, "DISMINUCION_PERSONAL", 9);
                                setIfNullDbg(newRow, iAUMENTO_JORNADALAB, "AUMENTO_JORNADALAB", 9);
                                setIfNullDbg(newRow, iDISMINUCION_JORNADALAB, "DISMINUCION_JORNADALAB", 9);
                                setIfNullDbg(newRow, iAUMENTO_SEMANA, "AUMENTO_SEMANA", 9);
                                setIfNullDbg(newRow, iDISMINUCION_SEMANA, "DISMINUCION_SEMANA", 9);
                                setIfNullDbg(newRow, iAUMENTO_SALARIOS, "AUMENTO_SALARIOS", 9);
                                setIfNullDbg(newRow, iDISMINUCION_SALARIOS, "DISMINUCION_SALARIOS", 9);
                                setIfNullDbg(newRow, iOTRO_TIPO, "OTRO_TIPO", 9);

                                Integer otroTipo = asInt(get(newRow, iOTRO_TIPO, "OTRO_TIPO"));
                                if (otroTipo != null && otroTipo == 1) {
                                    setIfNullDbg(newRow, iESPECIFIQUE_TIPO, "ESPECIFIQUE_TIPO", "No especifico");
                                }
                            }
                        }
                    }

                    if (estatusExp != null && estatusExp == 2) {
                        setIfNullDbg(newRow, iFECHA_ACTO_PROCESAL, "FECHA_ACTO_PROCESAL", D_1899);
                    }
                }
            }

            replace1999With1899Dbg(newRow, iFECHA_APERTURA_EXPEDIENTE, "FECHA_APERTURA_EXPEDIENTE");
            replace1999With1899Dbg(newRow, iFECHA_PRES_DEMANDA, "FECHA_PRES_DEMANDA");
            replace1999With1899Dbg(newRow, iFECHA_ADMISION_DEMANDA, "FECHA_ADMISION_DEMANDA");
            replace1999With1899Dbg(newRow, iFECHA_AUDIENCIA_ECONOM, "FECHA_AUDIENCIA_ECONOM");
            replace1999With1899Dbg(newRow, iFECHA_ACTO_PROCESAL, "FECHA_ACTO_PROCESAL");
            replace1999With1899Dbg(newRow, iFECHA_RESOLUCION, "FECHA_RESOLUCION");

             Integer tipoAsunto = asInt(newRow[iTIPO_ASUNTO]);            
              if (tipoAsunto != null && tipoAsunto == 1){
	String claveOrgano = asString(newRow[iCLAVE_ORGANO]);
        String expediente = asString(newRow[iEXPEDIENTE_CLAVE]);
          try ( PreparedStatement pe = conn.prepareStatement(Sql_Error)) {
            pe.setString(1, "V3_TR_COLECT_ECONOMJL");
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
                     && (fase == 1 || fase == 2 || fase == 3 || fase == 4
                     || fase == 5 || fase == 6 || fase == 7 || fase == 9)) {
                 String claveOrgano = asString(newRow[iCLAVE_ORGANO]);
                 String expediente = asString(newRow[iEXPEDIENTE_CLAVE]);
                 try ( PreparedStatement pe = conn.prepareStatement(Sql_Error)) {
                     pe.setString(1, "V3_TR_COLECT_ECONOMJL");
                     pe.setString(2, claveOrgano);
                     pe.setString(3, expediente);
                     pe.setString(4, "");
                     pe.setString(5, "");
                     pe.setInt(6, 0);
                     pe.setString(7, "El campo Fase_soli_expediente solo puede tener el valor=8.-Audiencia dentro del procedimiento colectivo de naturaleza económica");
                     pe.setString(8, "");
                     pe.executeUpdate();
                 } catch (SQLException ex) {
                     // Si hasta la tabla de errores falla, al menos lo imprimimos
                     System.err.println("❌ No se pudo guardar en ERRORES_INSERT: " + ex.getMessage());
                 }
          
            }
           
       } catch (Exception e) {
            dbg("EXCEPCION en trigger: " + e.getClass().getName() + " - " + e.getMessage());
            e.printStackTrace(System.out); // <-- AQUI veras la linea exacta
            throw e; // <-- importante: no te comas el error
        }
    }

    @Override public void close() { }
    @Override public void remove() { }
}
