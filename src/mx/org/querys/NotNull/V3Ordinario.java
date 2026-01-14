package mx.org.querys.NotNull;

import Conexion.ConexionH2;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class V3Ordinario {

    private static final Logger LOGGER = Logger.getLogger(V3Ordinario.class.getName());

    String sql;
    ArrayList<String[]> Array;

    /// Tipo de asunto no debe de ser =9 No_identificado
    public ArrayList Tipo_asuntoNI(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN TIPO_ASUNTO = 9 THEN 'No_identificado' ELSE CAST(TIPO_ASUNTO AS VARCHAR) END AS TIPO_ASUNTO, " +
              "PERIODO " +
              "FROM V3_TR_ORDINARIOJL " +
              "WHERE TIPO_ASUNTO = 9";

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
            LOGGER.log(Level.SEVERE, "Error en Tipo_asuntoNI()", ex);
        }
        return Array;
    }

    /// Cuando Tipo de asunto = Colectivo no debe capturar CONTRATO_ESCRITO ni TIPO_CONTRATO
    public ArrayList Tipo_asunto_ColectNI(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN TIPO_ASUNTO = 2 THEN 'Colectivo' ELSE CAST(TIPO_ASUNTO AS VARCHAR) END AS TIPO_ASUNTO, " +
              "PERIODO " +
              "FROM V3_TR_ORDINARIOJL " +
              "WHERE TIPO_ASUNTO = 2 " +
              "  AND (CONTRATO_ESCRITO IS NOT NULL OR TIPO_CONTRATO IS NOT NULL)";

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
            LOGGER.log(Level.SEVERE, "Error en Tipo_asunto_ColectNI()", ex);
        }
        return Array;
    }

    /// Cuando CONTRATO_ESCRITO = No o No identificado, no debe capturar TIPO_CONTRATO
    public ArrayList Contrato_escritoNI(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN CONTRATO_ESCRITO = 2 THEN 'No' " +
              "     WHEN CONTRATO_ESCRITO = 9 THEN 'No identificado' " +
              "     ELSE CAST(CONTRATO_ESCRITO AS VARCHAR) END AS CONTRATO_ESCRITO, " +
              "PERIODO " +
              "FROM V3_TR_ORDINARIOJL " +
              "WHERE TIPO_ASUNTO = 1 " +
              "  AND CONTRATO_ESCRITO IN (2,9) " +
              "  AND TIPO_CONTRATO IS NOT NULL";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("CONTRATO_ESCRITO"),
                    rs.getString("PERIODO")
                });
            }
        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Contrato_escritoNI()", ex);
        }
        return Array;
    }

    /// Cuando Tipo_asunto=Colectivo no debe capturarse desde SUBCONTRATACION... hasta OTRO_ESP_PRESTAC
    public ArrayList Ta_Colectivo(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN TIPO_ASUNTO = 2 THEN 'Colectivo' ELSE CAST(TIPO_ASUNTO AS VARCHAR) END AS CONTRATO_ESCRITO, " +
              "PERIODO " +
              "FROM V3_TR_ORDINARIOJL " +
              "WHERE TIPO_ASUNTO = 2 AND (" +
              "SUBCONTRATACION IS NOT NULL OR DESPIDO IS NOT NULL OR RESCISION_RL IS NOT NULL OR " +
              "TERMINACION_RESCISION_RL IS NOT NULL OR VIOLACION_CONTRATO IS NOT NULL OR RIESGO_TRABAJO IS NOT NULL OR " +
              "REVISION_CONTRATO IS NOT NULL OR PART_UTILIDADES IS NOT NULL OR OTRO_MOTIV_CONFLICTO IS NOT NULL OR " +
              "OTRO_ESP_CONFLICTO IS NOT NULL OR CIRCUNS_MOTIVO_CONFL IS NOT NULL OR DETERM_EMPLEO_EMBARAZO IS NOT NULL OR " +
              "DETERM_EMPLEO_EDAD IS NOT NULL OR DETERM_EMPLEO_GENERO IS NOT NULL OR DETERM_EMPLEO_ORIEN_SEX IS NOT NULL OR " +
              "DETERM_EMPLEO_DISCAPACIDAD IS NOT NULL OR DETERM_EMPLEO_SOCIAL IS NOT NULL OR DETERM_EMPLEO_ORIGEN IS NOT NULL OR " +
              "DETERM_EMPLEO_RELIGION IS NOT NULL OR DETERM_EMPLEO_MIGRA IS NOT NULL OR OTRO_DISCRIMINACION IS NOT NULL OR " +
              "OTRO_ESP_DISCRIMI IS NOT NULL OR TRATA_LABORAL IS NOT NULL OR TRABAJO_FORZOSO IS NOT NULL OR TRABAJO_INFANTIL IS NOT NULL OR " +
              "HOSTIGAMIENTO IS NOT NULL OR ACOSO_SEXUAL IS NOT NULL OR PAGO_PRESTACIONES IS NOT NULL OR INDEMNIZACION IS NOT NULL OR " +
              "REINSTALACION IS NOT NULL OR SALARIO_RETENIDO IS NOT NULL OR AUMENTO_SALARIO IS NOT NULL OR DERECHO_ASCENSO IS NOT NULL OR " +
              "DERECHO_PREFERENCIA IS NOT NULL OR DERECHO_ANTIGUEDAD IS NOT NULL OR OTRO_CONCEPTO IS NOT NULL OR OTRO_ESP_RECLAMADO IS NOT NULL OR " +
              "AGUINALDO IS NOT NULL OR VACACIONES IS NOT NULL OR PRIMA_VACACIONAL IS NOT NULL OR PRIMA_ANTIGUEDAD IS NOT NULL OR " +
              "OTRO_TIPO_PREST IS NOT NULL OR OTRO_ESP_PRESTAC IS NOT NULL)";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("CONTRATO_ESCRITO"),
                    rs.getString("PERIODO")
                });
            }
        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Ta_Colectivo()", ex);
        }
        return Array;
    }

    /// Cuando CIRCUNS_MOTIVO_CONFL = No/NI, no debe capturar discriminación/trata/hostigamiento...
    public ArrayList Mot_conflicto(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN CIRCUNS_MOTIVO_CONFL = 2 THEN 'No' " +
              "     WHEN CIRCUNS_MOTIVO_CONFL = 9 THEN 'No identificado' " +
              "     ELSE CAST(CIRCUNS_MOTIVO_CONFL AS VARCHAR) END AS CIRCUNS_MOTIVO_CONFL, " +
              "PERIODO " +
              "FROM V3_TR_ORDINARIOJL " +
              "WHERE TIPO_ASUNTO = 1 " +
              "  AND CIRCUNS_MOTIVO_CONFL IN (2,9) " +
              "  AND (DETERM_EMPLEO_EMBARAZO IS NOT NULL OR DETERM_EMPLEO_EDAD IS NOT NULL OR DETERM_EMPLEO_GENERO IS NOT NULL OR " +
              "       DETERM_EMPLEO_ORIEN_SEX IS NOT NULL OR DETERM_EMPLEO_DISCAPACIDAD IS NOT NULL OR DETERM_EMPLEO_SOCIAL IS NOT NULL OR " +
              "       DETERM_EMPLEO_ORIGEN IS NOT NULL OR DETERM_EMPLEO_RELIGION IS NOT NULL OR DETERM_EMPLEO_MIGRA IS NOT NULL OR " +
              "       OTRO_DISCRIMINACION IS NOT NULL OR OTRO_ESP_DISCRIMI IS NOT NULL OR TRATA_LABORAL IS NOT NULL OR " +
              "       TRABAJO_FORZOSO IS NOT NULL OR TRABAJO_INFANTIL IS NOT NULL OR HOSTIGAMIENTO IS NOT NULL OR ACOSO_SEXUAL IS NOT NULL)";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("CIRCUNS_MOTIVO_CONFL"),
                    rs.getString("PERIODO")
                });
            }
        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Mot_conflicto()", ex);
        }
        return Array;
    }

    /// Cuando PAGO_PRESTACIONES = No/NI, no debe capturar AGUINALDO..OTRO_ESP_PRESTAC
    public ArrayList Pago_prestaciones(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN PAGO_PRESTACIONES = 2 THEN 'No' " +
              "     WHEN PAGO_PRESTACIONES = 9 THEN 'No identificado' " +
              "     ELSE CAST(PAGO_PRESTACIONES AS VARCHAR) END AS PAGO_PRESTACIONES, " +
              "PERIODO " +
              "FROM V3_TR_ORDINARIOJL " +
              "WHERE TIPO_ASUNTO = 1 " +
              "  AND PAGO_PRESTACIONES IN (2,9) " +
              "  AND (AGUINALDO IS NOT NULL OR VACACIONES IS NOT NULL OR PRIMA_VACACIONAL IS NOT NULL OR " +
              "       PRIMA_ANTIGUEDAD IS NOT NULL OR OTRO_TIPO_PREST IS NOT NULL OR OTRO_ESP_PRESTAC IS NOT NULL)";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("PAGO_PRESTACIONES"),
                    rs.getString("PERIODO")
                });
            }
        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Pago_prestaciones()", ex);
        }
        return Array;
    }

    /// CONSTANCIA_CONS_EXPEDIDA = No/NI e INCOMPETENCIA=2, no debe capturar CONSTANCIA_CLAVE
    public ArrayList Cons_expedida(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN CONSTANCIA_CONS_EXPEDIDA = 2 THEN 'No' " +
              "     WHEN CONSTANCIA_CONS_EXPEDIDA = 9 THEN 'No identificado' " +
              "     ELSE CAST(CONSTANCIA_CONS_EXPEDIDA AS VARCHAR) END AS CONSTANCIA_CONS_EXPEDIDA, " +
              "PERIODO " +
              "FROM V3_TR_ORDINARIOJL " +
              "WHERE CONSTANCIA_CONS_EXPEDIDA IN (2,9) " +
              "  AND INCOMPETENCIA = 2 " +
              "  AND CONSTANCIA_CLAVE IS NOT NULL";

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
            LOGGER.log(Level.SEVERE, "Error en Cons_expedida()", ex);
        }
        return Array;
    }

    /// PREVE_DEMANDA = No/NI e INCOMPETENCIA=2, no debe capturar DESAHOGO_PREV_DEMANDA
    public ArrayList Preve_demanda(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN PREVE_DEMANDA = 2 THEN 'No' " +
              "     WHEN PREVE_DEMANDA = 9 THEN 'No identificado' " +
              "     ELSE CAST(PREVE_DEMANDA AS VARCHAR) END AS PREVE_DEMANDA, " +
              "PERIODO " +
              "FROM V3_TR_ORDINARIOJL " +
              "WHERE PREVE_DEMANDA IN (2,9) " +
              "  AND INCOMPETENCIA = 2 " +
              "  AND DESAHOGO_PREV_DEMANDA IS NOT NULL";

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
            LOGGER.log(Level.SEVERE, "Error en Preve_demanda()", ex);
        }
        return Array;
    }

    /// ESTATUS_DEMANDA no debe ser 9
    public ArrayList Estatus_demaNi(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN ESTATUS_DEMANDA = 9 THEN 'No_identificado' ELSE CAST(ESTATUS_DEMANDA AS VARCHAR) END AS ESTATUS_DEMANDA, " +
              "REPLACE(COMENTARIOS, ',', '') AS COMENTARIOS, " +
              "PERIODO " +
              "FROM V3_TR_ORDINARIOJL " +
              "WHERE ESTATUS_DEMANDA = 9";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("ESTATUS_DEMANDA"),
                    rs.getString("COMENTARIOS")
                });
            }
        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Estatus_demaNi()", ex);
        }
        return Array;
    }

    /// Estatus_demanda=Admitida o NI, INCOMPETENCIA=2, no debe capturar CAU_IMP_ADM_DEMANDA
    public ArrayList Impiden_admision_demanda(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN ESTATUS_DEMANDA = 1 THEN 'Admitida' " +
              "     WHEN ESTATUS_DEMANDA = 9 THEN 'No identificado' " +
              "     ELSE CAST(ESTATUS_DEMANDA AS VARCHAR) END AS ESTATUS_DEMANDA, " +
              "PERIODO " +
              "FROM V3_TR_ORDINARIOJL " +
              "WHERE ESTATUS_DEMANDA IN (1,9) " +
              "  AND INCOMPETENCIA = 2 " +
              "  AND CAU_IMP_ADM_DEMANDA IS NOT NULL";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("ESTATUS_DEMANDA"),
                    rs.getString("PERIODO")
                });
            }
        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Impiden_admision_demanda()", ex);
        }
        return Array;
    }

    /// AUDIENCIA_PRELIM = No/NI e INCOMPETENCIA=2, no debe capturar FECHA_AUDIENCIA_PRELIM
    public ArrayList audiencia_preliminar(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN AUDIENCIA_PRELIM = 2 THEN 'No' " +
              "     WHEN AUDIENCIA_PRELIM = 9 THEN 'No identificado' " +
              "     ELSE CAST(AUDIENCIA_PRELIM AS VARCHAR) END AS AUDIENCIA_PRELIM, " +
              "PERIODO " +
              "FROM V3_TR_ORDINARIOJL " +
              "WHERE AUDIENCIA_PRELIM IN (2,9) " +
              "  AND INCOMPETENCIA = 2 " +
              "  AND FECHA_AUDIENCIA_PRELIM IS NOT NULL";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("AUDIENCIA_PRELIM"),
                    rs.getString("PERIODO")
                });
            }
        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en audiencia_preliminar()", ex);
        }
        return Array;
    }

    /// AUDIENCIA_JUICIO = No/NI e INCOMPETENCIA=2, no debe capturar FECHA_AUDIENCIA_JUICIO
    public ArrayList audiencia_juicio(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN AUDIENCIA_JUICIO = 2 THEN 'No' " +
              "     WHEN AUDIENCIA_JUICIO = 9 THEN 'No identificado' " +
              "     ELSE CAST(AUDIENCIA_JUICIO AS VARCHAR) END AS AUDIENCIA_JUICIO, " +
              "PERIODO " +
              "FROM V3_TR_ORDINARIOJL " +
              "WHERE AUDIENCIA_JUICIO IN (2,9) " +
              "  AND INCOMPETENCIA = 2 " +
              "  AND FECHA_AUDIENCIA_JUICIO IS NOT NULL";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("AUDIENCIA_JUICIO"),
                    rs.getString("PERIODO")
                });
            }
        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en audiencia_juicio()", ex);
        }
        return Array;
    }

    /// ESTATUS_EXPEDIENTE no debe ser 9
    public ArrayList Estatus_ExpedienteNI(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN ESTATUS_EXPEDIENTE = 9 THEN 'No_identificado' ELSE CAST(ESTATUS_EXPEDIENTE AS VARCHAR) END AS ESTATUS_EXPEDIENTE, " +
              "REPLACE(COMENTARIOS, ',', '') AS COMENTARIOS, " +
              "PERIODO " +
              "FROM V3_TR_ORDINARIOJL " +
              "WHERE ESTATUS_EXPEDIENTE = 9";

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
            LOGGER.log(Level.SEVERE, "Error en Estatus_ExpedienteNI()", ex);
        }
        return Array;
    }

    /// INCOMPETENCIA no debe ser 9
    public ArrayList IncompetenciaNI(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, INCOMPETENCIA, " +
              "REPLACE(COMENTARIOS, ',', '') AS COMENTARIOS, PERIODO " +
              "FROM V3_TR_ORDINARIOJL " +
              "WHERE INCOMPETENCIA = 9";

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
            LOGGER.log(Level.SEVERE, "Error en IncompetenciaNI()", ex);
        }
        return Array;
    }

    /// INCOMPETENCIA=2 (No) no debe traer TIPO_INCOMPETENCIA
    public ArrayList Tipo_IncompetenciaNI(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN INCOMPETENCIA = 2 THEN 'No' ELSE CAST(INCOMPETENCIA AS VARCHAR) END AS INCOMPETENCIA, " +
              "TIPO_INCOMPETENCIA, PERIODO " +
              "FROM V3_TR_ORDINARIOJL " +
              "WHERE INCOMPETENCIA = 2 " +
              "  AND TIPO_INCOMPETENCIA IS NOT NULL";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                // en tu clase original SOLO devolvías 4 columnas (sin TIPO_INCOMPETENCIA)
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("INCOMPETENCIA"),
                    rs.getString("PERIODO")
                });
            }
        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Tipo_IncompetenciaNI()", ex);
        }
        return Array;
    }

    /// INCOMPETENCIA=1 y ya trae datos posteriores (pivote)
    public ArrayList PivIncompetencia(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, INCOMPETENCIA, PERIODO " +
              "FROM V3_TR_ORDINARIOJL " +
              "WHERE INCOMPETENCIA = 1 AND (" +
              "FECHA_PRES_DEMANDA IS NOT NULL OR CONSTANCIA_CONS_EXPEDIDA IS NOT NULL OR CONSTANCIA_CLAVE IS NOT NULL OR " +
              "ASUN_EXCEP_CONCILIACION IS NOT NULL OR PREVE_DEMANDA IS NOT NULL OR DESAHOGO_PREV_DEMANDA IS NOT NULL OR " +
              "ESTATUS_DEMANDA IS NOT NULL OR CAU_IMP_ADM_DEMANDA IS NOT NULL OR FECHA_ADMI_DEMANDA IS NOT NULL OR " +
              "CANTIDAD_ACTORES IS NOT NULL OR CANTIDAD_DEMANDADOS IS NOT NULL OR AUDIENCIA_PRELIM IS NOT NULL OR " +
              "FECHA_AUDIENCIA_PRELIM IS NOT NULL OR AUDIENCIA_JUICIO IS NOT NULL OR FECHA_AUDIENCIA_JUICIO IS NOT NULL OR " +
              "ESTATUS_EXPEDIENTE IS NOT NULL OR FECHA_ACTO_PROCESAL IS NOT NULL OR FASE_SOLI_EXPEDIENTE IS NOT NULL OR " +
              "FORMA_SOLUCIONFE IS NOT NULL OR OTRO_ESP_SOLUCIONFE IS NOT NULL OR FECHA_DICTO_RESOLUCIONFE IS NOT NULL OR MONTO_SOLUCION_FE IS NOT NULL OR " +
              "FORMA_SOLUCIONAP IS NOT NULL OR OTRO_ESP_SOLUCIONAP IS NOT NULL OR FECHA_DICTO_RESOLUCIONAP IS NOT NULL OR MONTO_SOLUCION_AP IS NOT NULL OR " +
              "FORMA_SOLUCIONAJ IS NOT NULL OR OTRO_ESP_SOLUCIONAJ IS NOT NULL OR FECHA_RESOLUCIONAJ IS NOT NULL OR TIPO_SENTENCIAAJ IS NOT NULL OR MONTO_SOLUCIONAJ IS NOT NULL" +
              ")";

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
            LOGGER.log(Level.SEVERE, "Error en PivIncompetencia()", ex);
        }
        return Array;
    }

    /// INCOMPETENCIA=9 y ya trae datos posteriores
    public ArrayList PivIncompetencia_Noidentificado(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, REPLACE(COMENTARIOS, ',', '') AS COMENTARIOS, PERIODO " +
              "FROM V3_TR_ORDINARIOJL " +
              "WHERE INCOMPETENCIA = 9 AND (" +
              "TIPO_INCOMPETENCIA IS NOT NULL OR OTRO_ESP_INCOMP IS NOT NULL OR FECHA_PRES_DEMANDA IS NOT NULL OR " +
              "CONSTANCIA_CONS_EXPEDIDA IS NOT NULL OR CONSTANCIA_CLAVE IS NOT NULL OR ASUN_EXCEP_CONCILIACION IS NOT NULL OR " +
              "PREVE_DEMANDA IS NOT NULL OR DESAHOGO_PREV_DEMANDA IS NOT NULL OR ESTATUS_DEMANDA IS NOT NULL OR " +
              "CAU_IMP_ADM_DEMANDA IS NOT NULL OR FECHA_ADMI_DEMANDA IS NOT NULL OR CANTIDAD_ACTORES IS NOT NULL OR " +
              "CANTIDAD_DEMANDADOS IS NOT NULL OR AUDIENCIA_PRELIM IS NOT NULL OR FECHA_AUDIENCIA_PRELIM IS NOT NULL OR " +
              "AUDIENCIA_JUICIO IS NOT NULL OR FECHA_AUDIENCIA_JUICIO IS NOT NULL OR ESTATUS_EXPEDIENTE IS NOT NULL OR " +
              "FECHA_ACTO_PROCESAL IS NOT NULL OR FASE_SOLI_EXPEDIENTE IS NOT NULL OR FORMA_SOLUCIONFE IS NOT NULL OR " +
              "OTRO_ESP_SOLUCIONFE IS NOT NULL OR FECHA_DICTO_RESOLUCIONFE IS NOT NULL OR MONTO_SOLUCION_FE IS NOT NULL OR " +
              "FORMA_SOLUCIONAP IS NOT NULL OR OTRO_ESP_SOLUCIONAP IS NOT NULL OR FECHA_DICTO_RESOLUCIONAP IS NOT NULL OR MONTO_SOLUCION_AP IS NOT NULL OR " +
              "FORMA_SOLUCIONAJ IS NOT NULL OR OTRO_ESP_SOLUCIONAJ IS NOT NULL OR FECHA_RESOLUCIONAJ IS NOT NULL OR TIPO_SENTENCIAAJ IS NOT NULL OR MONTO_SOLUCIONAJ IS NOT NULL" +
              ")";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("COMENTARIOS"),
                    rs.getString("PERIODO")
                });
            }
        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en PivIncompetencia_Noidentificado()", ex);
        }
        return Array;
    }

    /// ESTATUS_DEMANDA IN (2,3,4) y ya trae datos posteriores
    public ArrayList Estatus_Demanda_Desechada(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, ESTATUS_DEMANDA, PERIODO " +
              "FROM V3_TR_ORDINARIOJL " +
              "WHERE ESTATUS_DEMANDA IN (2,3,4) AND (" +
              "FECHA_ADMI_DEMANDA IS NOT NULL OR CANTIDAD_ACTORES IS NOT NULL OR CANTIDAD_DEMANDADOS IS NOT NULL OR " +
              "AUDIENCIA_PRELIM IS NOT NULL OR FECHA_AUDIENCIA_PRELIM IS NOT NULL OR AUDIENCIA_JUICIO IS NOT NULL OR FECHA_AUDIENCIA_JUICIO IS NOT NULL OR " +
              "ESTATUS_EXPEDIENTE IS NOT NULL OR FECHA_ACTO_PROCESAL IS NOT NULL OR FASE_SOLI_EXPEDIENTE IS NOT NULL OR " +
              "FORMA_SOLUCIONFE IS NOT NULL OR OTRO_ESP_SOLUCIONFE IS NOT NULL OR FECHA_DICTO_RESOLUCIONFE IS NOT NULL OR MONTO_SOLUCION_FE IS NOT NULL OR " +
              "FORMA_SOLUCIONAP IS NOT NULL OR OTRO_ESP_SOLUCIONAP IS NOT NULL OR FECHA_DICTO_RESOLUCIONAP IS NOT NULL OR MONTO_SOLUCION_AP IS NOT NULL OR " +
              "FORMA_SOLUCIONAJ IS NOT NULL OR OTRO_ESP_SOLUCIONAJ IS NOT NULL OR FECHA_RESOLUCIONAJ IS NOT NULL OR TIPO_SENTENCIAAJ IS NOT NULL OR MONTO_SOLUCIONAJ IS NOT NULL" +
              ")";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                // en tu clase original solo regresabas 3 columnas
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("ESTATUS_DEMANDA")
                });
            }
        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Estatus_Demanda_Desechada()", ex);
        }
        return Array;
    }

    /// ESTATUS_DEMANDA=9 y ya trae datos posteriores
    public ArrayList Estatus_Demanda_NoIdentificada(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, ESTATUS_DEMANDA, PERIODO " +
              "FROM V3_TR_ORDINARIOJL " +
              "WHERE ESTATUS_DEMANDA = 9 AND (" +
              "FECHA_ADMI_DEMANDA IS NOT NULL OR CANTIDAD_ACTORES IS NOT NULL OR CANTIDAD_DEMANDADOS IS NOT NULL OR " +
              "AUDIENCIA_PRELIM IS NOT NULL OR FECHA_AUDIENCIA_PRELIM IS NOT NULL OR AUDIENCIA_JUICIO IS NOT NULL OR FECHA_AUDIENCIA_JUICIO IS NOT NULL OR " +
              "ESTATUS_EXPEDIENTE IS NOT NULL OR FECHA_ACTO_PROCESAL IS NOT NULL OR FASE_SOLI_EXPEDIENTE IS NOT NULL OR " +
              "FORMA_SOLUCIONFE IS NOT NULL OR OTRO_ESP_SOLUCIONFE IS NOT NULL OR FECHA_DICTO_RESOLUCIONFE IS NOT NULL OR MONTO_SOLUCION_FE IS NOT NULL OR " +
              "FORMA_SOLUCIONAP IS NOT NULL OR OTRO_ESP_SOLUCIONAP IS NOT NULL OR FECHA_DICTO_RESOLUCIONAP IS NOT NULL OR MONTO_SOLUCION_AP IS NOT NULL OR " +
              "FORMA_SOLUCIONAJ IS NOT NULL OR OTRO_ESP_SOLUCIONAJ IS NOT NULL OR FECHA_RESOLUCIONAJ IS NOT NULL OR TIPO_SENTENCIAAJ IS NOT NULL OR MONTO_SOLUCIONAJ IS NOT NULL" +
              ")";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                // en tu clase original solo regresabas 3 columnas
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("ESTATUS_DEMANDA")
                });
            }
        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Estatus_Demanda_NoIdentificada()", ex);
        }
        return Array;
    }

    /* EN PROCESO O PREVENCION */
    /// FECHA_APERTURA_EXPEDIENTE excede 2 meses cuando ESTATUS_DEMANDA=5
    public ArrayList Estatus_Demanda_EnPrevenProceso(Connection con) {
        Array = new ArrayList<>();

        // En H2 reemplazamos: ADD_MONTHS(SYSDATE, -2) por DATEADD('MONTH', -2, CURRENT_DATE)
        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, FECHA_APERTURA_EXPEDIENTE, " +
              "'En trámite o prevención' AS ESTATUS_DEMANDA, " +
              "REPLACE(COMENTARIOS, ',', '') AS COMENTARIOS, PERIODO " +
              "FROM V3_TR_ORDINARIOJL " +
              "WHERE ESTATUS_DEMANDA = 5 " +
              "  AND FECHA_APERTURA_EXPEDIENTE < DATEADD('MONTH', -2, CURRENT_DATE)";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                // en tu clase original regresabas 3 columnas
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("ESTATUS_DEMANDA")
                });
            }
        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Estatus_Demanda_EnPrevenProceso()", ex);
        }
        return Array;
    }

    /// ESTATUS_EXPEDIENTE=2 y ya trae info posterior (no debería)
    public ArrayList Estatus_Expediente(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, ESTATUS_EXPEDIENTE, PERIODO " +
              "FROM V3_TR_ORDINARIOJL " +
              "WHERE ESTATUS_EXPEDIENTE = 2 AND (" +
              "FASE_SOLI_EXPEDIENTE IS NOT NULL OR FORMA_SOLUCIONFE IS NOT NULL OR OTRO_ESP_SOLUCIONFE IS NOT NULL OR " +
              "FECHA_DICTO_RESOLUCIONFE IS NOT NULL OR MONTO_SOLUCION_FE IS NOT NULL OR FORMA_SOLUCIONAP IS NOT NULL OR " +
              "OTRO_ESP_SOLUCIONAP IS NOT NULL OR FECHA_DICTO_RESOLUCIONAP IS NOT NULL OR MONTO_SOLUCION_AP IS NOT NULL OR " +
              "FORMA_SOLUCIONAJ IS NOT NULL OR OTRO_ESP_SOLUCIONAJ IS NOT NULL OR FECHA_RESOLUCIONAJ IS NOT NULL OR " +
              "TIPO_SENTENCIAAJ IS NOT NULL OR MONTO_SOLUCIONAJ IS NOT NULL)";

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
            LOGGER.log(Level.SEVERE, "Error en Estatus_Expediente()", ex);
        }
        return Array;
    }

    /// ESTATUS_EXPEDIENTE=9 y ya trae info posterior (no debería)
    public ArrayList Estatus_Expediente_Noidentificado(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, ESTATUS_EXPEDIENTE, PERIODO " +
              "FROM V3_TR_ORDINARIOJL " +
              "WHERE ESTATUS_EXPEDIENTE = 9 AND (" +
              "FASE_SOLI_EXPEDIENTE IS NOT NULL OR FECHA_ACTO_PROCESAL IS NOT NULL OR FORMA_SOLUCIONFE IS NOT NULL OR " +
              "OTRO_ESP_SOLUCIONFE IS NOT NULL OR FECHA_DICTO_RESOLUCIONFE IS NOT NULL OR MONTO_SOLUCION_FE IS NOT NULL OR " +
              "FORMA_SOLUCIONAP IS NOT NULL OR OTRO_ESP_SOLUCIONAP IS NOT NULL OR FECHA_DICTO_RESOLUCIONAP IS NOT NULL OR MONTO_SOLUCION_AP IS NOT NULL OR " +
              "FORMA_SOLUCIONAJ IS NOT NULL OR OTRO_ESP_SOLUCIONAJ IS NOT NULL OR FECHA_RESOLUCIONAJ IS NOT NULL OR " +
              "TIPO_SENTENCIAAJ IS NOT NULL OR MONTO_SOLUCIONAJ IS NOT NULL)";

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
            LOGGER.log(Level.SEVERE, "Error en Estatus_Expediente_Noidentificado()", ex);
        }
        return Array;
    }

    /// ESTATUS_EXPEDIENTE=1 y FECHA_ACTO_PROCESAL no nula
    public ArrayList Fecha_acto_procesal(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, ESTATUS_EXPEDIENTE, FECHA_ACTO_PROCESAL, PERIODO " +
              "FROM V3_TR_ORDINARIOJL " +
              "WHERE ESTATUS_EXPEDIENTE = 1 AND FECHA_ACTO_PROCESAL IS NOT NULL";

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
            LOGGER.log(Level.SEVERE, "Error en Fecha_acto_procesal()", ex);
        }
        return Array;
    }

    public ArrayList Fase_Sol_expNoExiste(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT P.CLAVE_ORGANO, P.EXPEDIENTE_CLAVE, S.DESCRIPCION AS FASE_SOLI_EXPEDIENTE " +
              "FROM V3_TR_ORDINARIOJL P " +
              "JOIN V3_TC_FASE_EXPEDIENTEJL S ON P.FASE_SOLI_EXPEDIENTE = S.ID " +
              "WHERE P.FASE_SOLI_EXPEDIENTE NOT IN (1,2,9,99)";

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
            LOGGER.log(Level.SEVERE, "Error en Fase_Sol_expNoExiste()", ex);
        }
        return Array;
    }

    public ArrayList Fase_Sol_expNI(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN FASE_SOLI_EXPEDIENTE = 99 THEN 'No identificado' ELSE CAST(FASE_SOLI_EXPEDIENTE AS VARCHAR) END AS FASE_SOLI_EXPEDIENTE, " +
              "PERIODO " +
              "FROM V3_TR_ORDINARIOJL " +
              "WHERE FASE_SOLI_EXPEDIENTE = 99 AND (" +
              "FORMA_SOLUCIONFE IS NOT NULL OR OTRO_ESP_SOLUCIONFE IS NOT NULL OR FECHA_DICTO_RESOLUCIONFE IS NOT NULL OR MONTO_SOLUCION_FE IS NOT NULL OR " +
              "FORMA_SOLUCIONAP IS NOT NULL OR OTRO_ESP_SOLUCIONAP IS NOT NULL OR FECHA_DICTO_RESOLUCIONAP IS NOT NULL OR MONTO_SOLUCION_AP IS NOT NULL OR " +
              "FORMA_SOLUCIONAJ IS NOT NULL OR OTRO_ESP_SOLUCIONAJ IS NOT NULL OR FECHA_RESOLUCIONAJ IS NOT NULL OR TIPO_SENTENCIAAJ IS NOT NULL OR MONTO_SOLUCIONAJ IS NOT NULL)";

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
            LOGGER.log(Level.SEVERE, "Error en Fase_Sol_expNI()", ex);
        }
        return Array;
    }

    public ArrayList Fase_Sol_exp_FE(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, FASE_SOLI_EXPEDIENTE, PERIODO " +
              "FROM V3_TR_ORDINARIOJL " +
              "WHERE FASE_SOLI_EXPEDIENTE = 9 AND (" +
              "FORMA_SOLUCIONAP IS NOT NULL OR OTRO_ESP_SOLUCIONAP IS NOT NULL OR FECHA_DICTO_RESOLUCIONAP IS NOT NULL OR MONTO_SOLUCION_AP IS NOT NULL OR " +
              "FORMA_SOLUCIONAJ IS NOT NULL OR OTRO_ESP_SOLUCIONAJ IS NOT NULL OR FECHA_RESOLUCIONAJ IS NOT NULL OR TIPO_SENTENCIAAJ IS NOT NULL OR MONTO_SOLUCIONAJ IS NOT NULL)";

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
            LOGGER.log(Level.SEVERE, "Error en Fase_Sol_exp_FE()", ex);
        }
        return Array;
    }

    public ArrayList Fase_Sol_exp_AP(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, FASE_SOLI_EXPEDIENTE, PERIODO " +
              "FROM V3_TR_ORDINARIOJL " +
              "WHERE FASE_SOLI_EXPEDIENTE = 1 AND (" +
              "FORMA_SOLUCIONFE IS NOT NULL OR OTRO_ESP_SOLUCIONFE IS NOT NULL OR FECHA_DICTO_RESOLUCIONFE IS NOT NULL OR MONTO_SOLUCION_FE IS NOT NULL OR " +
              "FORMA_SOLUCIONAJ IS NOT NULL OR OTRO_ESP_SOLUCIONAJ IS NOT NULL OR FECHA_RESOLUCIONAJ IS NOT NULL OR TIPO_SENTENCIAAJ IS NOT NULL OR MONTO_SOLUCIONAJ IS NOT NULL)";

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
            LOGGER.log(Level.SEVERE, "Error en Fase_Sol_exp_AP()", ex);
        }
        return Array;
    }

    public ArrayList Fase_Sol_exp_AJ(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, FASE_SOLI_EXPEDIENTE, PERIODO " +
              "FROM V3_TR_ORDINARIOJL " +
              "WHERE FASE_SOLI_EXPEDIENTE = 2 AND (" +
              "FORMA_SOLUCIONAP IS NOT NULL OR OTRO_ESP_SOLUCIONAP IS NOT NULL OR FECHA_DICTO_RESOLUCIONAP IS NOT NULL OR MONTO_SOLUCION_AP IS NOT NULL OR " +
              "FORMA_SOLUCIONFE IS NOT NULL OR OTRO_ESP_SOLUCIONFE IS NOT NULL OR FECHA_DICTO_RESOLUCIONFE IS NOT NULL OR MONTO_SOLUCION_FE IS NOT NULL)";

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
            LOGGER.log(Level.SEVERE, "Error en Fase_Sol_exp_AJ()", ex);
        }
        return Array;
    }

    public ArrayList SinMotivo_Conflicto(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "FORMATDATETIME(FECHA_APERTURA_EXPEDIENTE,'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE " +
              "FROM V3_TR_ORDINARIOJL " +
              "WHERE DESPIDO = 2 AND RESCISION_RL = 2 AND TERMINACION_RESCISION_RL = 2 AND VIOLACION_CONTRATO = 2 AND " +
              "RIESGO_TRABAJO = 2 AND REVISION_CONTRATO = 2 AND PART_UTILIDADES = 2 AND OTRO_MOTIV_CONFLICTO = 2 AND " +
              "CIRCUNS_MOTIVO_CONFL = 1";

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
            LOGGER.log(Level.SEVERE, "Error en SinMotivo_Conflicto()", ex);
        }
        return Array;
    }
}
