/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mx.org.querys.NotNull;

import Conexion.ConexionH2;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author ANTONIO.CORIA
 */
public class V3Colectivo {

    private static final Logger LOG = Logger.getLogger(V3Colectivo.class.getName());

    String sql;
    ArrayList<String[]> Array;

    // Tipo de asunto no debe de ser =9 No_identificado
    public ArrayList Tipo_Asunto(Connection con) {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, "
            + "CASE WHEN TIPO_ASUNTO = 9 THEN 'No_identificado' ELSE NULL END AS TIPO_ASUNTO, "
            + "PERIODO "
            + "FROM V3_TR_COLECTIVOJL "
            + "WHERE TIPO_ASUNTO = 9";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("TIPO_ASUNTO"),
                    rs.getString("PERIODO")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Tipo_Asunto()", ex);
        }
        return Array;
    }

    // Cuando constancia expedida = NO o NI y además INCOMPETENCIA=1, no debe existir CONSTANCIA_CLAVE
    public ArrayList Cons_Expedida(Connection con) {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, "
            + "CASE "
            + "  WHEN CONSTANCIA_CONS_EXPEDIDA = 2 THEN 'No' "
            + "  WHEN CONSTANCIA_CONS_EXPEDIDA = 9 THEN 'No identificado' "
            + "  ELSE NULL "
            + "END AS CONSTANCIA_CONS_EXPEDIDA, "
            + "PERIODO "
            + "FROM V3_TR_COLECTIVOJL "
            + "WHERE CONSTANCIA_CONS_EXPEDIDA IN (2,9) "
            + "  AND INCOMPETENCIA = 1 "
            + "  AND CONSTANCIA_CLAVE IS NOT NULL";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("CONSTANCIA_CONS_EXPEDIDA"),
                    rs.getString("PERIODO")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Cons_Expedida()", ex);
        }
        return Array;
    }

    // Suspensión temporal = No o NI, no debe capturar campos relacionados (solo si TIPO_ASUNTO=2)
    public ArrayList Suspencion_temporal(Connection con) {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, "
            + "CASE "
            + "  WHEN SUSPENSION_TMP = 2 THEN 'No' "
            + "  WHEN SUSPENSION_TMP = 9 THEN 'No identificado' "
            + "  ELSE NULL "
            + "END AS SUSPENSION_TMP, "
            + "PERIODO "
            + "FROM V3_TR_COLECTIVOJL "
            + "WHERE SUSPENSION_TMP IN (2,9) "
            + "  AND TIPO_ASUNTO = 2 "
            + "  AND (NO_IMPUTABLE_ST IS NOT NULL "
            + "       OR INCAPACIDAD_FISICA_ST IS NOT NULL "
            + "       OR FALTA_MATERIA_PRIM_ST IS NOT NULL "
            + "       OR FALTA_MINISTRACION_ST IS NOT NULL)";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("SUSPENSION_TMP"),
                    rs.getString("PERIODO")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Suspencion_temporal()", ex);
        }
        return Array;
    }

    // Terminación colectiva = No o NI, no debe capturar causas (solo si TIPO_ASUNTO=2)
    public ArrayList Terminacion_Colectiva(Connection con) {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, "
            + "CASE "
            + "  WHEN TERMINACION_TRAB = 2 THEN 'No' "
            + "  WHEN TERMINACION_TRAB = 9 THEN 'No identificado' "
            + "  ELSE NULL "
            + "END AS TERMINACION_TRAB, "
            + "PERIODO "
            + "FROM V3_TR_COLECTIVOJL "
            + "WHERE TERMINACION_TRAB IN (2,9) "
            + "  AND TIPO_ASUNTO = 2 "
            + "  AND (FUERZA_MAYOR_TC IS NOT NULL "
            + "       OR INCAPACIDAD_FISICA_TC IS NOT NULL "
            + "       OR QUIEBRA_LEGAL_TC IS NOT NULL "
            + "       OR AGOTAMIENTO_MATERIA_TC IS NOT NULL)";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("TERMINACION_TRAB"),
                    rs.getString("PERIODO")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Terminacion_Colectiva()", ex);
        }
        return Array;
    }

    // Violación derechos = No (según tu query original usa =2) y no debe capturar campos relacionados (si TIPO_ASUNTO=2)
    public ArrayList Viola_Derechos(Connection con) {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, "
            + "CASE "
            + "  WHEN VIOLA_DERECHOS = 2 THEN 'No' "
            + "  WHEN VIOLA_DERECHOS = 9 THEN 'No identificado' "
            + "  ELSE NULL "
            + "END AS VIOLA_DERECHOS, "
            + "PERIODO "
            + "FROM V3_TR_COLECTIVOJL "
            + "WHERE VIOLA_DERECHOS = 2 "
            + "  AND TIPO_ASUNTO = 2 "
            + "  AND (LIBERTAD_ASOCIACION IS NOT NULL "
            + "       OR LIBERTAD_SINDICAL IS NOT NULL "
            + "       OR DERECHO_COLECTIVA IS NOT NULL "
            + "       OR OTRO_COLECTIVA IS NOT NULL "
            + "       OR OTRO_ESP_COLECTIVA IS NOT NULL)";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("VIOLA_DERECHOS"),
                    rs.getString("PERIODO")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Viola_Derechos()", ex);
        }
        return Array;
    }

    // Estatus demanda no debe ser 9
    public ArrayList Estatus_Demanda(Connection con) {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, "
            + "CASE WHEN ESTATUS_DEMANDA = 9 THEN 'No_identificado' ELSE NULL END AS ESTATUS_DEMANDA, "
            + "REPLACE(COMENTARIOS, ',', '') AS COMENTARIOS, "
            + "PERIODO "
            + "FROM V3_TR_COLECTIVOJL "
            + "WHERE ESTATUS_DEMANDA = 9";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("ESTATUS_DEMANDA"),
                    rs.getString("COMENTARIOS"),
                    rs.getString("PERIODO")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Estatus_Demanda()", ex);
        }
        return Array;
    }

    // Estatus demanda = 5 y fecha_apertura_expediente excede 2 meses
    public ArrayList Estatus_Demanda_PrevenProceso(Connection con) {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, FECHA_APERTURA_EXPEDIENTE, "
            + "CASE WHEN ESTATUS_DEMANDA = 5 THEN 'En trámite o prevención' ELSE NULL END AS ESTATUS_DEMANDA, "
            + "REPLACE(COMENTARIOS, ',', '') AS COMENTARIOS, "
            + "PERIODO "
            + "FROM V3_TR_COLECTIVOJL "
            + "WHERE ESTATUS_DEMANDA = 5 "
            + "  AND FECHA_APERTURA_EXPEDIENTE < DATEADD('MONTH', -2, CURRENT_DATE)";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("ESTATUS_DEMANDA")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Estatus_Demanda_PrevenProceso()", ex);
        }
        return Array;
    }

    // Estatus expediente no debe ser 9
    public ArrayList Estatus_ExpedienteNI(Connection con) {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, "
            + "CASE WHEN ESTATUS_EXPEDIENTE = 9 THEN 'No_identificado' ELSE NULL END AS ESTATUS_EXPEDIENTE, "
            + "REPLACE(COMENTARIOS, ',', '') AS COMENTARIOS, "
            + "PERIODO "
            + "FROM V3_TR_COLECTIVOJL "
            + "WHERE ESTATUS_EXPEDIENTE = 9";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("ESTATUS_EXPEDIENTE"),
                    rs.getString("COMENTARIOS"),
                    rs.getString("PERIODO")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Estatus_ExpedienteNI()", ex);
        }
        return Array;
    }

    // Incompetencia no debe ser 9
    public ArrayList IncompetenciaNI(Connection con) {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, INCOMPETENCIA, "
            + "REPLACE(COMENTARIOS, ',', '') AS COMENTARIOS, "
            + "PERIODO "
            + "FROM V3_TR_COLECTIVOJL "
            + "WHERE INCOMPETENCIA = 9";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("INCOMPETENCIA"),
                    rs.getString("COMENTARIOS"),
                    rs.getString("PERIODO")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en IncompetenciaNI()", ex);
        }
        return Array;
    }

    // Si INCOMPETENCIA=2 (No) entonces TIPO_INCOMPETENCIA no debe venir capturado
    public ArrayList Tipo_IncompetenciaNI(Connection con) {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, "
            + "CASE WHEN INCOMPETENCIA = 2 THEN 'No' ELSE NULL END AS INCOMPETENCIA, "
            + "TIPO_INCOMPETENCIA, "
            + "PERIODO "
            + "FROM V3_TR_COLECTIVOJL "
            + "WHERE INCOMPETENCIA = 2 "
            + "  AND TIPO_INCOMPETENCIA IS NOT NULL";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("INCOMPETENCIA"),
                    rs.getString("PERIODO")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Tipo_IncompetenciaNI()", ex);
        }
        return Array;
    }

    // INCOMPETENCIA=1 pero trae campos capturados (pivote)
    public ArrayList PivIncompetencia(Connection con) {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, INCOMPETENCIA, PERIODO "
            + "FROM V3_TR_COLECTIVOJL "
            + "WHERE INCOMPETENCIA = 1 "
            + "  AND (FECHA_PRES_DEMANDA IS NOT NULL OR "
            + "       CONSTANCIA_CONS_EXPEDIDA IS NOT NULL OR "
            + "       CONSTANCIA_CLAVE IS NOT NULL OR "
            + "       ASUN_EXCEP_CONCILIACION IS NOT NULL OR "
            + "       PREVE_DEMANDA IS NOT NULL OR "
            + "       DESAHOGO_PREV_DEMANDA IS NOT NULL OR "
            + "       ESTATUS_DEMANDA IS NOT NULL OR "
            + "       FECHA_ADMI_DEMANDA IS NOT NULL OR "
            + "       CANTIDAD_ACTORES IS NOT NULL OR "
            + "       CANTIDAD_DEMANDADOS IS NOT NULL OR "
            + "       AUTO_DEPURACION IS NOT NULL OR "
            + "       FECHA_DEPURACION IS NOT NULL OR "
            + "       AUDIENCIA_JUICIO IS NOT NULL OR "
            + "       FECHA_AUDIENCIA_JUICIO IS NOT NULL OR "
            + "       ESTATUS_EXPEDIENTE IS NOT NULL OR "
            + "       FECHA_ACTO_PROCESAL IS NOT NULL OR "
            + "       FASE_SOLI_EXPEDIENTE IS NOT NULL OR "
            + "       FORMA_SOLUCION_AD IS NOT NULL OR "
            + "       OTRO_ESP_SOLUCION_AD IS NOT NULL OR "
            + "       FECHA_DICTO_RESOLUCION_AD IS NOT NULL OR "
            + "       TIPO_SENTENCIA_AD IS NOT NULL OR "
            + "       MONTO_SOLUCION_AD IS NOT NULL OR "
            + "       FORMA_SOLUCION_AJ IS NOT NULL OR "
            + "       OTRO_ESP_SOLUCION_AJ IS NOT NULL OR "
            + "       FECHA_RESOLUCION_AJ IS NOT NULL OR "
            + "       TIPO_SENTENCIA_AJ IS NOT NULL OR "
            + "       MONTO_SOLUCION_AJ IS NOT NULL)";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("INCOMPETENCIA"),
                    rs.getString("PERIODO")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en PivIncompetencia()", ex);
        }
        return Array;
    }

    // PREVE_DEMANDA = No o NI y DESAHOGO_PREV_DEMANDA viene capturado
    public ArrayList Preve_demanda(Connection con) {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, "
            + "CASE "
            + "  WHEN PREVE_DEMANDA = 2 THEN 'No' "
            + "  WHEN PREVE_DEMANDA = 9 THEN 'No identificado' "
            + "  ELSE NULL "
            + "END AS PREVE_DEMANDA, "
            + "PERIODO "
            + "FROM V3_TR_COLECTIVOJL "
            + "WHERE PREVE_DEMANDA IN (2,9) "
            + "  AND INCOMPETENCIA = 2 "
            + "  AND DESAHOGO_PREV_DEMANDA IS NOT NULL";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("PREVE_DEMANDA"),
                    rs.getString("PERIODO")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Preve_demanda()", ex);
        }
        return Array;
    }

    // ESTATUS_DEMANDA = 2,3,4 pero trae campos posteriores capturados
    public ArrayList Estatus_Demanda_Desechada(Connection con) {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, ESTATUS_DEMANDA "
            + "FROM V3_TR_COLECTIVOJL "
            + "WHERE ESTATUS_DEMANDA IN (2,3,4) "
            + "  AND (FECHA_ADMI_DEMANDA IS NOT NULL OR "
            + "       CANTIDAD_ACTORES IS NOT NULL OR "
            + "       CANTIDAD_DEMANDADOS IS NOT NULL OR "
            + "       AUTO_DEPURACION IS NOT NULL OR "
            + "       FECHA_DEPURACION IS NOT NULL OR "
            + "       AUDIENCIA_JUICIO IS NOT NULL OR "
            + "       FECHA_AUDIENCIA_JUICIO IS NOT NULL OR "
            + "       ESTATUS_EXPEDIENTE IS NOT NULL OR "
            + "       FECHA_ACTO_PROCESAL IS NOT NULL OR "
            + "       FASE_SOLI_EXPEDIENTE IS NOT NULL OR "
            + "       FORMA_SOLUCION_AD IS NOT NULL OR "
            + "       OTRO_ESP_SOLUCION_AD IS NOT NULL OR "
            + "       FECHA_DICTO_RESOLUCION_AD IS NOT NULL OR "
            + "       TIPO_SENTENCIA_AD IS NOT NULL OR "
            + "       MONTO_SOLUCION_AD IS NOT NULL OR "
            + "       FORMA_SOLUCION_AJ IS NOT NULL OR "
            + "       OTRO_ESP_SOLUCION_AJ IS NOT NULL OR "
            + "       FECHA_RESOLUCION_AJ IS NOT NULL OR "
            + "       TIPO_SENTENCIA_AJ IS NOT NULL OR "
            + "       MONTO_SOLUCION_AJ IS NOT NULL)";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("ESTATUS_DEMANDA")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Estatus_Demanda_Desechada()", ex);
        }
        return Array;
    }

    // AUTO_DEPURACION = No o NI y trae FECHA_DEPURACION
    public ArrayList Tram_depuracion(Connection con) {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, "
            + "CASE "
            + "  WHEN AUTO_DEPURACION = 2 THEN 'No' "
            + "  WHEN AUTO_DEPURACION = 9 THEN 'No identificado' "
            + "  ELSE NULL "
            + "END AS AUTO_DEPURACION, "
            + "PERIODO "
            + "FROM V3_TR_COLECTIVOJL "
            + "WHERE AUTO_DEPURACION IN (2,9) "
            + "  AND ESTATUS_DEMANDA = 1 "
            + "  AND FECHA_DEPURACION IS NOT NULL";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("AUTO_DEPURACION")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Tram_depuracion()", ex);
        }
        return Array;
    }

    // AUDIENCIA_JUICIO = No o NI y trae FECHA_AUDIENCIA_JUICIO
    public ArrayList Audiencia_juicio(Connection con) {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, "
            + "CASE "
            + "  WHEN AUDIENCIA_JUICIO = 2 THEN 'No' "
            + "  WHEN AUDIENCIA_JUICIO = 9 THEN 'No identificado' "
            + "  ELSE NULL "
            + "END AS AUDIENCIA_JUICIO, "
            + "PERIODO "
            + "FROM V3_TR_COLECTIVOJL "
            + "WHERE AUDIENCIA_JUICIO IN (2,9) "
            + "  AND ESTATUS_DEMANDA = 1 "
            + "  AND FECHA_AUDIENCIA_JUICIO IS NOT NULL";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("AUDIENCIA_JUICIO")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Audiencia_juicio()", ex);
        }
        return Array;
    }

    // Estatus expediente = 2 pero trae datos de resolución
    public ArrayList Estatus_Expediente(Connection con) {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, ESTATUS_EXPEDIENTE "
            + "FROM V3_TR_COLECTIVOJL "
            + "WHERE ESTATUS_EXPEDIENTE = 2 "
            + "  AND (FASE_SOLI_EXPEDIENTE IS NOT NULL OR "
            + "       FORMA_SOLUCION_AD IS NOT NULL OR "
            + "       OTRO_ESP_SOLUCION_AD IS NOT NULL OR "
            + "       FECHA_DICTO_RESOLUCION_AD IS NOT NULL OR "
            + "       TIPO_SENTENCIA_AD IS NOT NULL OR "
            + "       MONTO_SOLUCION_AD IS NOT NULL OR "
            + "       FORMA_SOLUCION_AJ IS NOT NULL OR "
            + "       OTRO_ESP_SOLUCION_AJ IS NOT NULL OR "
            + "       FECHA_RESOLUCION_AJ IS NOT NULL OR "
            + "       TIPO_SENTENCIA_AJ IS NOT NULL OR "
            + "       MONTO_SOLUCION_AJ IS NOT NULL)";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("ESTATUS_EXPEDIENTE")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Estatus_Expediente()", ex);
        }
        return Array;
    }

    // Estatus expediente = 1 (solucionado) pero trae FECHA_ACTO_PROCESAL
    public ArrayList Fecha_acto_procesal(Connection con) {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, ESTATUS_EXPEDIENTE, FECHA_ACTO_PROCESAL "
            + "FROM V3_TR_COLECTIVOJL "
            + "WHERE ESTATUS_EXPEDIENTE = 1 "
            + "  AND FECHA_ACTO_PROCESAL IS NOT NULL";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("ESTATUS_EXPEDIENTE"),
                    rs.getString("FECHA_ACTO_PROCESAL")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Fecha_acto_procesal()", ex);
        }
        return Array;
    }

    public ArrayList Fase_Sol_expNoExiste(Connection con) {
        Array = new ArrayList();

        // Mantengo tu join estilo Oracle (coma) pero en H2 también funciona.
        sql = "SELECT P.CLAVE_ORGANO, P.EXPEDIENTE_CLAVE, S.DESCRIPCION AS FASE_SOLI_EXPEDIENTE "
            + "FROM V3_TR_COLECTIVOJL P, V3_TC_FASE_EXPEDIENTEJL S "
            + "WHERE P.FASE_SOLI_EXPEDIENTE = S.ID "
            + "  AND P.FASE_SOLI_EXPEDIENTE NOT IN (2,3,99)";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FASE_SOLI_EXPEDIENTE")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Fase_Sol_expNoExiste()", ex);
        }
        return Array;
    }

    // FASE_SOLI_EXPEDIENTE = 99 y trae datos de solución
    public ArrayList Fase_Sol_expNI(Connection con) {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, "
            + "CASE WHEN FASE_SOLI_EXPEDIENTE = 99 THEN 'No identificado' ELSE NULL END AS FASE_SOLI_EXPEDIENTE, "
            + "PERIODO "
            + "FROM V3_TR_COLECTIVOJL "
            + "WHERE FASE_SOLI_EXPEDIENTE = 99 "
            + "  AND (FORMA_SOLUCION_AD IS NOT NULL OR OTRO_ESP_SOLUCION_AD IS NOT NULL OR "
            + "       FECHA_DICTO_RESOLUCION_AD IS NOT NULL OR TIPO_SENTENCIA_AD IS NOT NULL OR MONTO_SOLUCION_AD IS NOT NULL OR "
            + "       FORMA_SOLUCION_AJ IS NOT NULL OR OTRO_ESP_SOLUCION_AJ IS NOT NULL OR FECHA_RESOLUCION_AJ IS NOT NULL OR "
            + "       TIPO_SENTENCIA_AJ IS NOT NULL OR MONTO_SOLUCION_AJ IS NOT NULL)";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FASE_SOLI_EXPEDIENTE")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Fase_Sol_expNI()", ex);
        }
        return Array;
    }

    // FASE_SOLI_EXPEDIENTE = 3 (Tramitación por auto depuración) pero trae solución AJ
    public ArrayList Fase_Sol_exp_TAP(Connection con) {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, FASE_SOLI_EXPEDIENTE "
            + "FROM V3_TR_COLECTIVOJL "
            + "WHERE FASE_SOLI_EXPEDIENTE IN (3) "
            + "  AND (FORMA_SOLUCION_AJ IS NOT NULL OR OTRO_ESP_SOLUCION_AJ IS NOT NULL OR FECHA_RESOLUCION_AJ IS NOT NULL OR "
            + "       TIPO_SENTENCIA_AJ IS NOT NULL OR MONTO_SOLUCION_AJ IS NOT NULL)";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FASE_SOLI_EXPEDIENTE")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Fase_Sol_exp_TAP()", ex);
        }
        return Array;
    }

    // FASE_SOLI_EXPEDIENTE = 2 (Audiencia juicio) pero trae solución AD
    public ArrayList Fase_Sol_exp_AJ(Connection con) {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, FASE_SOLI_EXPEDIENTE "
            + "FROM V3_TR_COLECTIVOJL "
            + "WHERE FASE_SOLI_EXPEDIENTE IN (2) "
            + "  AND (FORMA_SOLUCION_AD IS NOT NULL OR OTRO_ESP_SOLUCION_AD IS NOT NULL OR "
            + "       FECHA_DICTO_RESOLUCION_AD IS NOT NULL OR MONTO_SOLUCION_AD IS NOT NULL)";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FASE_SOLI_EXPEDIENTE")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Fase_Sol_exp_AJ()", ex);
        }
        return Array;
    }

    // Sin motivo de conflicto (todo = 2)
    public ArrayList SinMotivo_Conflicto(Connection con) {
        Array = new ArrayList();

        // Tu query traía TO_CHAR(fecha,'dd/mm/yyyy'), en H2 mejor FORMATDATETIME
        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, "
            + "FORMATDATETIME(FECHA_APERTURA_EXPEDIENTE, 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE "
            + "FROM V3_TR_COLECTIVOJL "
            + "WHERE DECLARACION_PERDIDA_MAY = 2 "
            + "  AND SUSPENSION_TMP = 2 "
            + "  AND TERMINACION_TRAB = 2 "
            + "  AND CONTRATACION_COLECTIVA = 2 "
            + "  AND OMISIONES_REGLAMENTO = 2 "
            + "  AND REDUCCION_PERSONAL = 2 "
            + "  AND VIOLA_DERECHOS = 2 "
            + "  AND ELECCION_SINDICALES = 2 "
            + "  AND SANCION_SINDICALES = 2 "
            + "  AND OTRO_CONFLICTO = 2";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en SinMotivo_Conflicto()", ex);
        }
        return Array;
    }
}
