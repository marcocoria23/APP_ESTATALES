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
public class V3Individual {

    String sql;
    ArrayList<String[]> Array;

    private static final Logger LOGGER = Logger.getLogger(V3Individual.class.getName());

    ///Tipo de asunto no debe de ser =9 No_identificado
    public ArrayList Tipo_Asunto() {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN TIPO_ASUNTO = 9 THEN 'No_identificado' ELSE CAST(TIPO_ASUNTO AS VARCHAR) END AS TIPO_ASUNTO " +
              "FROM V3_TR_INDIVIDUALJL " +
              "WHERE TIPO_ASUNTO = 9";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("TIPO_ASUNTO")
                });
            }
        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Tipo_Asunto()", ex);
        }
        return Array;
    }

    //Cuando ¿El trabajador contó con contrato escrito? = No o No identificado, no debe de capturar tipo de contrato
    public ArrayList Contrato_Escrito() {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE " +
              "  WHEN CONTRATO_ESCRITO = 2 THEN 'No' " +
              "  WHEN CONTRATO_ESCRITO = 9 THEN 'No identificado' " +
              "  ELSE CAST(CONTRATO_ESCRITO AS VARCHAR) " +
              "END AS CONTRATO_ESCRITO " +
              "FROM V3_TR_INDIVIDUALJL " +
              "WHERE CONTRATO_ESCRITO IN (2,9) AND TIPO_CONTRATO IS NOT NULL";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("CONTRATO_ESCRITO")
                });
            }
        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Contrato_Escrito()", ex);
        }
        return Array;
    }

    ////Cuando ¿Se anexó constancia de no conciliación... ? = NO O NO IDENTIFICADO no debe capturar constancia_clave
    public ArrayList Centro_conciliacion() {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE " +
              "  WHEN CONSTANCIA_CONS_EXPEDIDA = 2 THEN 'No' " +
              "  WHEN CONSTANCIA_CONS_EXPEDIDA = 9 THEN 'No identificado' " +
              "  ELSE CAST(CONSTANCIA_CONS_EXPEDIDA AS VARCHAR) " +
              "END AS CONSTANCIA_CONS_EXPEDIDA " +
              "FROM V3_TR_INDIVIDUALJL " +
              "WHERE CONSTANCIA_CONS_EXPEDIDA IN (2,9) " +
              "  AND INCOMPETENCIA = 2 " +
              "  AND CONSTANCIA_CLAVE IS NOT NULL";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("CONSTANCIA_CONS_EXPEDIDA")
                });
            }
        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Centro_conciliacion()", ex);
        }
        return Array;
    }

    ////Cuando ¿Se formuló prevención a la demanda? = NO/NO IDENTIFICADO no debe capturar DESAHOGO_PREV_DEMANDA
    public ArrayList Preve_demanda() {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE " +
              "  WHEN PREVE_DEMANDA = 2 THEN 'No' " +
              "  WHEN PREVE_DEMANDA = 9 THEN 'No identificado' " +
              "  ELSE CAST(PREVE_DEMANDA AS VARCHAR) " +
              "END AS PREVE_DEMANDA " +
              "FROM V3_TR_INDIVIDUALJL " +
              "WHERE PREVE_DEMANDA IN (2,9) " +
              "  AND INCOMPETENCIA = 2 " +
              "  AND DESAHOGO_PREV_DEMANDA IS NOT NULL";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("PREVE_DEMANDA")
                });
            }
        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Preve_demanda()", ex);
        }
        return Array;
    }

    ////Estatus de la demanda no debe de ser 9=No_identificado.
    public ArrayList Estatus_Demanda() {
        Array = new ArrayList<>();

        // Nota: el NOT IN se conserva pero sin filtros PValidacion
        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN ESTATUS_DEMANDA = 9 THEN 'No_identificado' ELSE CAST(ESTATUS_DEMANDA AS VARCHAR) END AS ESTATUS_DEMANDA, " +
              "REPLACE(COMENTARIOS, ',', '') AS COMENTARIOS " +
              "FROM V3_TR_INDIVIDUALJL " +
              "WHERE ESTATUS_DEMANDA = 9 " +
              "  AND (CLAVE_ORGANO || EXPEDIENTE_CLAVE) NOT IN ('12035106/2022')";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
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
            LOGGER.log(Level.SEVERE, "Error en Estatus_Demanda()", ex);
        }
        return Array;
    }

    ////Cuando Estatus de la demanda =Admitida no debe capturarse Causas que impiden la admisión
    public ArrayList Estatus_Demanda_admitida() {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN ESTATUS_DEMANDA = 1 THEN 'Admitida' ELSE CAST(ESTATUS_DEMANDA AS VARCHAR) END AS ESTATUS_DEMANDA " +
              "FROM V3_TR_INDIVIDUALJL " +
              "WHERE ESTATUS_DEMANDA = 1 AND CAU_IMPI_ADMI_DEMANDA IS NOT NULL";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
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
            LOGGER.log(Level.SEVERE, "Error en Estatus_Demanda_admitida()", ex);
        }
        return Array;
    }

    ////Tramitación por auto de depuración = No/No identificado, no debe capturar FECHA_DEPURACION
    public ArrayList Tram_depuracion() {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE " +
              "  WHEN TRAMITACION_DEPURACION = 2 THEN 'No' " +
              "  WHEN TRAMITACION_DEPURACION = 9 THEN 'No identificado' " +
              "  ELSE CAST(TRAMITACION_DEPURACION AS VARCHAR) " +
              "END AS TRAMITACION_DEPURACION " +
              "FROM V3_TR_INDIVIDUALJL " +
              "WHERE TRAMITACION_DEPURACION IN (2,9) " +
              "  AND ESTATUS_DEMANDA = 1 " +
              "  AND FECHA_DEPURACION IS NOT NULL";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("TRAMITACION_DEPURACION")
                });
            }
        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Tram_depuracion()", ex);
        }
        return Array;
    }

    ////Audiencia preliminar = No/No identificado, no debe capturar FECHA_AUDIENCIA_PRELIM
    public ArrayList Audiencia_preliminar() {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE " +
              "  WHEN AUDIENCIA_PRELIM = 2 THEN 'No' " +
              "  WHEN AUDIENCIA_PRELIM = 9 THEN 'No identificado' " +
              "  ELSE CAST(AUDIENCIA_PRELIM AS VARCHAR) " +
              "END AS AUDIENCIA_PRELIM " +
              "FROM V3_TR_INDIVIDUALJL " +
              "WHERE AUDIENCIA_PRELIM IN (2,9) " +
              "  AND ESTATUS_DEMANDA = 1 " +
              "  AND FECHA_AUDIENCIA_PRELIM IS NOT NULL";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("AUDIENCIA_PRELIM")
                });
            }
        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Audiencia_preliminar()", ex);
        }
        return Array;
    }

    ////Audiencia de juicio = No/No identificado, no debe capturar FECHA_AUDIENCIA_JUICIO
    public ArrayList Audiencia_juicio() {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE " +
              "  WHEN AUDIENCIA_JUICIO = 2 THEN 'No' " +
              "  WHEN AUDIENCIA_JUICIO = 9 THEN 'No identificado' " +
              "  ELSE CAST(AUDIENCIA_JUICIO AS VARCHAR) " +
              "END AS AUDIENCIA_JUICIO " +
              "FROM V3_TR_INDIVIDUALJL " +
              "WHERE AUDIENCIA_JUICIO IN (2,9) " +
              "  AND ESTATUS_DEMANDA = 1 " +
              "  AND FECHA_AUDIENCIA_JUICIO IS NOT NULL";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
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
            LOGGER.log(Level.SEVERE, "Error en Audiencia_juicio()", ex);
        }
        return Array;
    }

    ////Estatus del expediente no debe de ser 9=No_identificado.
    public ArrayList Estatus_ExpedienteNI() {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN ESTATUS_EXPEDIENTE = 9 THEN 'No identificado' ELSE CAST(ESTATUS_EXPEDIENTE AS VARCHAR) END AS ESTATUS_EXPEDIENTE, " +
              "REPLACE(COMENTARIOS, ',', '') AS COMENTARIOS " +
              "FROM V3_TR_INDIVIDUALJL " +
              "WHERE ESTATUS_EXPEDIENTE = 9";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("ESTATUS_EXPEDIENTE"),
                    rs.getString("COMENTARIOS")
                });
            }
        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Estatus_ExpedienteNI()", ex);
        }
        return Array;
    }

    ///INCOMPETENCIA NO DEBE SER = NO IDENTIFICADA
    public ArrayList IncompetenciaNI() {
        Array = new ArrayList<>();

        // En H2 ya no hace falta el subselect, pero lo dejo simple y equivalente:
        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, INCOMPETENCIA, " +
              "REPLACE(COMENTARIOS, ',', '') AS COMENTARIOS " +
              "FROM V3_TR_INDIVIDUALJL " +
              "WHERE INCOMPETENCIA = 9";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("INCOMPETENCIA"),
                    rs.getString("COMENTARIOS")
                });
            }
        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en IncompetenciaNI()", ex);
        }
        return Array;
    }

    ///INCOMPETENCIA=2 (No) y trae TIPO_INCOMPETENCIA -> no debe tenerlo
    public ArrayList Tipo_IncompetenciaNI() {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN INCOMPETENCIA = 2 THEN 'No' ELSE CAST(INCOMPETENCIA AS VARCHAR) END AS INCOMPETENCIA " +
              "FROM V3_TR_INDIVIDUALJL " +
              "WHERE INCOMPETENCIA = 2 AND TIPO_INCOMPETENCIA IS NOT NULL";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("INCOMPETENCIA")
                });
            }
        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Tipo_IncompetenciaNI()", ex);
        }
        return Array;
    }

    //INCOMPETENCIA=SI no debe capturar datos posteriores
    public ArrayList PivIncompetencia() {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, INCOMPETENCIA " +
              "FROM V3_TR_INDIVIDUALJL " +
              "WHERE INCOMPETENCIA = 1 AND ( " +
              "FECHA_PRES_DEMANDA IS NOT NULL OR CONSTANCIA_CONS_EXPEDIDA IS NOT NULL OR CONSTANCIA_CLAVE IS NOT NULL OR " +
              "ASUN_EXCEP_CONCILIACION IS NOT NULL OR PREVE_DEMANDA IS NOT NULL OR DESAHOGO_PREV_DEMANDA IS NOT NULL OR " +
              "ESTATUS_DEMANDA IS NOT NULL OR CAU_IMPI_ADMI_DEMANDA IS NOT NULL OR FECHA_ADMI_DEMANDA IS NOT NULL OR " +
              "CANTIDAD_ACTORES IS NOT NULL OR CANTIDAD_DEMANDADOS IS NOT NULL OR TRAMITACION_DEPURACION IS NOT NULL OR " +
              "FECHA_DEPURACION IS NOT NULL OR AUDIENCIA_PRELIM IS NOT NULL OR FECHA_AUDIENCIA_PRELIM IS NOT NULL OR AUDIENCIA_JUICIO IS NOT NULL OR " +
              "FECHA_AUDIENCIA_JUICIO IS NOT NULL OR ESTATUS_EXPEDIENTE IS NOT NULL OR FECHA_ACTO_PROCESAL IS NOT NULL OR " +
              "FASE_SOLI_EXPEDIENTE IS NOT NULL OR FORMA_SOLUCION_AD IS NOT NULL OR OTRO_ESP_SOLUCION_AD IS NOT NULL OR " +
              "FECHA_DICTO_RESOLUCION_AD IS NOT NULL OR TIPO_SENTENCIA_AD IS NOT NULL OR MONTO_SOLUCION_AD IS NOT NULL OR " +
              "FORMA_SOLUCION_TA IS NOT NULL OR OTRO_ESP_SOLUCION_TA IS NOT NULL OR " +
              "FECHA_RESOLUCION_TA IS NOT NULL OR TIPO_SENTENCIA_TA IS NOT NULL OR " +
              "MONTO_SOLUCION_TA IS NOT NULL OR FORMA_SOLUCION_AP IS NOT NULL OR OTRO_ESP_SOLUCION_AP IS NOT NULL OR " +
              "FECHA_DICTO_RESOLUCION_AP IS NOT NULL OR MONTO_SOLUCION_AP IS NOT NULL OR FORMA_SOLUCION_AJ IS NOT NULL OR " +
              "OTRO_ESP_SOLUCION_AJ IS NOT NULL OR FECHA_DICTO_RESOLUCION_AJ IS NOT NULL OR TIPO_SENTENCIA_AJ IS NOT NULL OR MONTO_SOLUCION_AJ IS NOT NULL " +
              ")";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("INCOMPETENCIA")
                });
            }
        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en PivIncompetencia()", ex);
        }
        return Array;
    }

    //INCOMPETENCIA=9 (No identificado) pero hay datos a partir de tipo_incompetencia y demás
    public ArrayList PivIncompetencia_Noindentificado() {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, INCOMPETENCIA " +
              "FROM V3_TR_INDIVIDUALJL " +
              "WHERE INCOMPETENCIA = 9 AND ( " +
              "TIPO_INCOMPETENCIA IS NOT NULL OR OTRO_ESP_INCOMP IS NOT NULL OR FECHA_PRES_DEMANDA IS NOT NULL OR " +
              "CONSTANCIA_CONS_EXPEDIDA IS NOT NULL OR CONSTANCIA_CLAVE IS NOT NULL OR ASUN_EXCEP_CONCILIACION IS NOT NULL OR " +
              "PREVE_DEMANDA IS NOT NULL OR DESAHOGO_PREV_DEMANDA IS NOT NULL OR ESTATUS_DEMANDA IS NOT NULL OR " +
              "CAU_IMPI_ADMI_DEMANDA IS NOT NULL OR FECHA_ADMI_DEMANDA IS NOT NULL OR CANTIDAD_ACTORES IS NOT NULL OR " +
              "CANTIDAD_DEMANDADOS IS NOT NULL OR TRAMITACION_DEPURACION IS NOT NULL OR FECHA_DEPURACION IS NOT NULL OR " +
              "AUDIENCIA_PRELIM IS NOT NULL OR FECHA_AUDIENCIA_PRELIM IS NOT NULL OR AUDIENCIA_JUICIO IS NOT NULL OR " +
              "FECHA_AUDIENCIA_JUICIO IS NOT NULL OR ESTATUS_EXPEDIENTE IS NOT NULL OR FECHA_ACTO_PROCESAL IS NOT NULL OR " +
              "FASE_SOLI_EXPEDIENTE IS NOT NULL OR FORMA_SOLUCION_AD IS NOT NULL OR OTRO_ESP_SOLUCION_AD IS NOT NULL OR " +
              "FECHA_DICTO_RESOLUCION_AD IS NOT NULL OR TIPO_SENTENCIA_AD IS NOT NULL OR MONTO_SOLUCION_AD IS NOT NULL OR " +
              "FORMA_SOLUCION_TA IS NOT NULL OR OTRO_ESP_SOLUCION_TA IS NOT NULL OR FECHA_RESOLUCION_TA IS NOT NULL OR " +
              "TIPO_SENTENCIA_TA IS NOT NULL OR MONTO_SOLUCION_TA IS NOT NULL OR FORMA_SOLUCION_AP IS NOT NULL OR " +
              "OTRO_ESP_SOLUCION_AP IS NOT NULL OR FECHA_DICTO_RESOLUCION_AP IS NOT NULL OR MONTO_SOLUCION_AP IS NOT NULL OR " +
              "FORMA_SOLUCION_AJ IS NOT NULL OR OTRO_ESP_SOLUCION_AJ IS NOT NULL OR FECHA_DICTO_RESOLUCION_AJ IS NOT NULL OR " +
              "TIPO_SENTENCIA_AJ IS NOT NULL OR MONTO_SOLUCION_AJ IS NOT NULL " +
              ")";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("INCOMPETENCIA")
                });
            }
        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en PivIncompetencia_Noindentificado()", ex);
        }
        return Array;
    }

    //Estatus demanda desechada/archivo/no se dio trámite (2,3,4) y hay datos posteriores
    public ArrayList Estatus_Demanda_Desechada() {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, ESTATUS_DEMANDA " +
              "FROM V3_TR_INDIVIDUALJL " +
              "WHERE ESTATUS_DEMANDA IN (2,3,4) AND ( " +
              "FECHA_ADMI_DEMANDA IS NOT NULL OR CANTIDAD_ACTORES IS NOT NULL OR CANTIDAD_DEMANDADOS IS NOT NULL OR " +
              "TRAMITACION_DEPURACION IS NOT NULL OR FECHA_DEPURACION IS NOT NULL OR AUDIENCIA_PRELIM IS NOT NULL OR " +
              "FECHA_AUDIENCIA_PRELIM IS NOT NULL OR AUDIENCIA_JUICIO IS NOT NULL OR FECHA_AUDIENCIA_JUICIO IS NOT NULL OR " +
              "ESTATUS_EXPEDIENTE IS NOT NULL OR FECHA_ACTO_PROCESAL IS NOT NULL OR FASE_SOLI_EXPEDIENTE IS NOT NULL OR " +
              "FORMA_SOLUCION_AD IS NOT NULL OR OTRO_ESP_SOLUCION_AD IS NOT NULL OR FECHA_DICTO_RESOLUCION_AD IS NOT NULL OR " +
              "TIPO_SENTENCIA_AD IS NOT NULL OR MONTO_SOLUCION_AD IS NOT NULL OR FORMA_SOLUCION_TA IS NOT NULL OR " +
              "OTRO_ESP_SOLUCION_TA IS NOT NULL OR FECHA_RESOLUCION_TA IS NOT NULL OR TIPO_SENTENCIA_TA IS NOT NULL OR " +
              "MONTO_SOLUCION_TA IS NOT NULL OR FORMA_SOLUCION_AP IS NOT NULL OR OTRO_ESP_SOLUCION_AP IS NOT NULL OR " +
              "FECHA_DICTO_RESOLUCION_AP IS NOT NULL OR MONTO_SOLUCION_AP IS NOT NULL OR FORMA_SOLUCION_AJ IS NOT NULL OR " +
              "OTRO_ESP_SOLUCION_AJ IS NOT NULL OR FECHA_DICTO_RESOLUCION_AJ IS NOT NULL OR TIPO_SENTENCIA_AJ IS NOT NULL OR " +
              "MONTO_SOLUCION_AJ IS NOT NULL " +
              ")";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
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
            LOGGER.log(Level.SEVERE, "Error en Estatus_Demanda_Desechada()", ex);
        }
        return Array;
    }

    //Estatus demanda = 9 y hay datos posteriores
    public ArrayList Estatus_Demanda_Noidentificada() {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, ESTATUS_DEMANDA " +
              "FROM V3_TR_INDIVIDUALJL " +
              "WHERE ESTATUS_DEMANDA = 9 AND ( " +
              "FECHA_ADMI_DEMANDA IS NOT NULL OR CANTIDAD_ACTORES IS NOT NULL OR CANTIDAD_DEMANDADOS IS NOT NULL OR " +
              "TRAMITACION_DEPURACION IS NOT NULL OR FECHA_DEPURACION IS NOT NULL OR AUDIENCIA_PRELIM IS NOT NULL OR " +
              "FECHA_AUDIENCIA_PRELIM IS NOT NULL OR AUDIENCIA_JUICIO IS NOT NULL OR FECHA_AUDIENCIA_JUICIO IS NOT NULL OR " +
              "ESTATUS_EXPEDIENTE IS NOT NULL OR FECHA_ACTO_PROCESAL IS NOT NULL OR FASE_SOLI_EXPEDIENTE IS NOT NULL OR " +
              "FORMA_SOLUCION_AD IS NOT NULL OR OTRO_ESP_SOLUCION_AD IS NOT NULL OR FECHA_DICTO_RESOLUCION_AD IS NOT NULL OR " +
              "TIPO_SENTENCIA_AD IS NOT NULL OR MONTO_SOLUCION_AD IS NOT NULL OR FORMA_SOLUCION_TA IS NOT NULL OR " +
              "OTRO_ESP_SOLUCION_TA IS NOT NULL OR FECHA_RESOLUCION_TA IS NOT NULL OR TIPO_SENTENCIA_TA IS NOT NULL OR " +
              "MONTO_SOLUCION_TA IS NOT NULL OR FORMA_SOLUCION_AP IS NOT NULL OR OTRO_ESP_SOLUCION_AP IS NOT NULL OR " +
              "FECHA_DICTO_RESOLUCION_AP IS NOT NULL OR MONTO_SOLUCION_AP IS NOT NULL OR FORMA_SOLUCION_AJ IS NOT NULL OR " +
              "OTRO_ESP_SOLUCION_AJ IS NOT NULL OR FECHA_DICTO_RESOLUCION_AJ IS NOT NULL OR TIPO_SENTENCIA_AJ IS NOT NULL OR " +
              "MONTO_SOLUCION_AJ IS NOT NULL " +
              ")";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
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
            LOGGER.log(Level.SEVERE, "Error en Estatus_Demanda_Noidentificada()", ex);
        }
        return Array;
    }

    //Estatus demanda = 5 y fecha apertura excede 2 meses (H2: DATEADD)
    public ArrayList Estatus_Demanda_PrevenProceso() {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN ESTATUS_DEMANDA = 5 THEN 'En trámite o prevención' ELSE CAST(ESTATUS_DEMANDA AS VARCHAR) END AS ESTATUS_DEMANDA " +
              "FROM V3_TR_INDIVIDUALJL " +
              "WHERE ESTATUS_DEMANDA = 5 " +
              "  AND FECHA_APERTURA_EXPEDIENTE < DATEADD('MONTH', -2, CURRENT_DATE)";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
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
            LOGGER.log(Level.SEVERE, "Error en Estatus_Demanda_PrevenProceso()", ex);
        }
        return Array;
    }

    //ESTATUS EXPEDIENTE = 2 pero hay campos de solución
    public ArrayList Estatus_Expediente() {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, ESTATUS_EXPEDIENTE " +
              "FROM V3_TR_INDIVIDUALJL " +
              "WHERE ESTATUS_EXPEDIENTE = 2 AND ( " +
              "FASE_SOLI_EXPEDIENTE IS NOT NULL OR FORMA_SOLUCION_AD IS NOT NULL OR OTRO_ESP_SOLUCION_AD IS NOT NULL OR " +
              "FECHA_DICTO_RESOLUCION_AD IS NOT NULL OR TIPO_SENTENCIA_AD IS NOT NULL OR MONTO_SOLUCION_AD IS NOT NULL OR " +
              "FORMA_SOLUCION_TA IS NOT NULL OR OTRO_ESP_SOLUCION_TA IS NOT NULL OR FECHA_RESOLUCION_TA IS NOT NULL OR " +
              "TIPO_SENTENCIA_TA IS NOT NULL OR MONTO_SOLUCION_TA IS NOT NULL OR FORMA_SOLUCION_AP IS NOT NULL OR " +
              "OTRO_ESP_SOLUCION_AP IS NOT NULL OR FECHA_DICTO_RESOLUCION_AP IS NOT NULL OR MONTO_SOLUCION_AP IS NOT NULL OR " +
              "FORMA_SOLUCION_AJ IS NOT NULL OR OTRO_ESP_SOLUCION_AJ IS NOT NULL OR FECHA_DICTO_RESOLUCION_AJ IS NOT NULL OR " +
              "TIPO_SENTENCIA_AJ IS NOT NULL OR MONTO_SOLUCION_AJ IS NOT NULL " +
              ")";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
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

    //ESTATUS EXPEDIENTE = 9 pero hay campos contestados
    public ArrayList Estatus_Expediente_Noidentificado() {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, ESTATUS_EXPEDIENTE " +
              "FROM V3_TR_INDIVIDUALJL " +
              "WHERE ESTATUS_EXPEDIENTE = 9 AND ( " +
              "FASE_SOLI_EXPEDIENTE IS NOT NULL OR FORMA_SOLUCION_AD IS NOT NULL OR FECHA_ACTO_PROCESAL IS NOT NULL OR " +
              "OTRO_ESP_SOLUCION_AD IS NOT NULL OR FECHA_DICTO_RESOLUCION_AD IS NOT NULL OR TIPO_SENTENCIA_AD IS NOT NULL OR " +
              "MONTO_SOLUCION_AD IS NOT NULL OR FORMA_SOLUCION_TA IS NOT NULL OR OTRO_ESP_SOLUCION_TA IS NOT NULL OR " +
              "FECHA_RESOLUCION_TA IS NOT NULL OR TIPO_SENTENCIA_TA IS NOT NULL OR MONTO_SOLUCION_TA IS NOT NULL OR " +
              "FORMA_SOLUCION_AP IS NOT NULL OR OTRO_ESP_SOLUCION_AP IS NOT NULL OR FECHA_DICTO_RESOLUCION_AP IS NOT NULL OR " +
              "MONTO_SOLUCION_AP IS NOT NULL OR FORMA_SOLUCION_AJ IS NOT NULL OR OTRO_ESP_SOLUCION_AJ IS NOT NULL OR " +
              "FECHA_DICTO_RESOLUCION_AJ IS NOT NULL OR TIPO_SENTENCIA_AJ IS NOT NULL OR MONTO_SOLUCION_AJ IS NOT NULL " +
              ")";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
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

    //cuando el estatus del expediente es = solucionado no debe de haber nada en fecha del ultimo acto procesal
    public ArrayList Fecha_acto_procesal() {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, ESTATUS_EXPEDIENTE, FECHA_ACTO_PROCESAL " +
              "FROM V3_TR_INDIVIDUALJL " +
              "WHERE ESTATUS_EXPEDIENTE = 1 AND FECHA_ACTO_PROCESAL IS NOT NULL";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
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

    public ArrayList Fase_Sol_expNoExiste() {
        Array = new ArrayList<>();

        sql = "SELECT P.CLAVE_ORGANO, P.EXPEDIENTE_CLAVE, S.DESCRIPCION AS FASE_SOLI_EXPEDIENTE " +
              "FROM V3_TR_INDIVIDUALJL P, V3_TC_FASE_EXPEDIENTEJL S " +
              "WHERE P.FASE_SOLI_EXPEDIENTE = S.ID " +
              "AND P.FASE_SOLI_EXPEDIENTE NOT IN (1,2,3,4,99)";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
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

    public ArrayList Fase_Sol_expNI() {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN FASE_SOLI_EXPEDIENTE = 99 THEN 'No identificado' ELSE CAST(FASE_SOLI_EXPEDIENTE AS VARCHAR) END AS FASE_SOLI_EXPEDIENTE " +
              "FROM V3_TR_INDIVIDUALJL " +
              "WHERE FASE_SOLI_EXPEDIENTE = 99 AND ( " +
              "FORMA_SOLUCION_AD IS NOT NULL OR OTRO_ESP_SOLUCION_AD IS NOT NULL OR FECHA_DICTO_RESOLUCION_AD IS NOT NULL OR " +
              "TIPO_SENTENCIA_AD IS NOT NULL OR MONTO_SOLUCION_AD IS NOT NULL OR FORMA_SOLUCION_TA IS NOT NULL OR OTRO_ESP_SOLUCION_TA IS NOT NULL OR " +
              "FECHA_RESOLUCION_TA IS NOT NULL OR TIPO_SENTENCIA_TA IS NOT NULL OR MONTO_SOLUCION_TA IS NOT NULL OR FORMA_SOLUCION_AP IS NOT NULL OR " +
              "OTRO_ESP_SOLUCION_AP IS NOT NULL OR FECHA_DICTO_RESOLUCION_AP IS NOT NULL OR MONTO_SOLUCION_AP IS NOT NULL OR FORMA_SOLUCION_AJ IS NOT NULL OR " +
              "OTRO_ESP_SOLUCION_AJ IS NOT NULL OR FECHA_DICTO_RESOLUCION_AJ IS NOT NULL OR TIPO_SENTENCIA_AJ IS NOT NULL OR MONTO_SOLUCION_AJ IS NOT NULL " +
              ")";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
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

    public ArrayList Fase_Sol_exp_TD() {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, FASE_SOLI_EXPEDIENTE " +
              "FROM V3_TR_INDIVIDUALJL " +
              "WHERE FASE_SOLI_EXPEDIENTE IN (3) AND ( " +
              "FORMA_SOLUCION_TA IS NOT NULL OR OTRO_ESP_SOLUCION_TA IS NOT NULL OR FECHA_RESOLUCION_TA IS NOT NULL OR " +
              "TIPO_SENTENCIA_TA IS NOT NULL OR MONTO_SOLUCION_TA IS NOT NULL OR FORMA_SOLUCION_AP IS NOT NULL OR " +
              "OTRO_ESP_SOLUCION_AP IS NOT NULL OR FECHA_DICTO_RESOLUCION_AP IS NOT NULL OR MONTO_SOLUCION_AP IS NOT NULL OR " +
              "FORMA_SOLUCION_AJ IS NOT NULL OR OTRO_ESP_SOLUCION_AJ IS NOT NULL OR FECHA_DICTO_RESOLUCION_AJ IS NOT NULL OR " +
              "TIPO_SENTENCIA_AJ IS NOT NULL OR MONTO_SOLUCION_AJ IS NOT NULL " +
              ")";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
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
            LOGGER.log(Level.SEVERE, "Error en Fase_Sol_exp_TD()", ex);
        }
        return Array;
    }

    public ArrayList Fase_Sol_exp_TA() {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, FASE_SOLI_EXPEDIENTE " +
              "FROM V3_TR_INDIVIDUALJL " +
              "WHERE FASE_SOLI_EXPEDIENTE IN (4) AND ( " +
              "FORMA_SOLUCION_AD IS NOT NULL OR OTRO_ESP_SOLUCION_AD IS NOT NULL OR FECHA_DICTO_RESOLUCION_AD IS NOT NULL OR " +
              "TIPO_SENTENCIA_AD IS NOT NULL OR MONTO_SOLUCION_AD IS NOT NULL OR FORMA_SOLUCION_AP IS NOT NULL OR OTRO_ESP_SOLUCION_AP IS NOT NULL OR " +
              "FECHA_DICTO_RESOLUCION_AP IS NOT NULL OR MONTO_SOLUCION_AP IS NOT NULL OR FORMA_SOLUCION_AJ IS NOT NULL OR " +
              "OTRO_ESP_SOLUCION_AJ IS NOT NULL OR FECHA_DICTO_RESOLUCION_AJ IS NOT NULL OR TIPO_SENTENCIA_AJ IS NOT NULL OR MONTO_SOLUCION_AJ IS NOT NULL " +
              ")";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
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
            LOGGER.log(Level.SEVERE, "Error en Fase_Sol_exp_TA()", ex);
        }
        return Array;
    }

    public ArrayList Fase_Sol_exp_AP() {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, FASE_SOLI_EXPEDIENTE " +
              "FROM V3_TR_INDIVIDUALJL " +
              "WHERE FASE_SOLI_EXPEDIENTE IN (1) AND ( " +
              "FORMA_SOLUCION_AD IS NOT NULL OR OTRO_ESP_SOLUCION_AD IS NOT NULL OR FECHA_DICTO_RESOLUCION_AD IS NOT NULL OR " +
              "TIPO_SENTENCIA_AD IS NOT NULL OR MONTO_SOLUCION_AD IS NOT NULL OR FORMA_SOLUCION_TA IS NOT NULL OR OTRO_ESP_SOLUCION_TA IS NOT NULL OR " +
              "FECHA_RESOLUCION_TA IS NOT NULL OR TIPO_SENTENCIA_TA IS NOT NULL OR MONTO_SOLUCION_TA IS NOT NULL OR FORMA_SOLUCION_AJ IS NOT NULL OR " +
              "OTRO_ESP_SOLUCION_AJ IS NOT NULL OR FECHA_DICTO_RESOLUCION_AJ IS NOT NULL OR TIPO_SENTENCIA_AJ IS NOT NULL OR MONTO_SOLUCION_AJ IS NOT NULL " +
              ")";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
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

    public ArrayList Fase_Sol_exp_AJ() {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, FASE_SOLI_EXPEDIENTE " +
              "FROM V3_TR_INDIVIDUALJL " +
              "WHERE FASE_SOLI_EXPEDIENTE IN (2) AND ( " +
              "FORMA_SOLUCION_AD IS NOT NULL OR OTRO_ESP_SOLUCION_AD IS NOT NULL OR FECHA_DICTO_RESOLUCION_AD IS NOT NULL OR " +
              "TIPO_SENTENCIA_AD IS NOT NULL OR MONTO_SOLUCION_AD IS NOT NULL OR FORMA_SOLUCION_TA IS NOT NULL OR OTRO_ESP_SOLUCION_TA IS NOT NULL OR " +
              "FECHA_RESOLUCION_TA IS NOT NULL OR TIPO_SENTENCIA_TA IS NOT NULL OR MONTO_SOLUCION_TA IS NOT NULL OR FORMA_SOLUCION_AP IS NOT NULL OR " +
              "OTRO_ESP_SOLUCION_AP IS NOT NULL OR FECHA_DICTO_RESOLUCION_AP IS NOT NULL OR MONTO_SOLUCION_AP IS NOT NULL " +
              ")";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
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

    public ArrayList SinMotivo_Conflicto() {
        Array = new ArrayList<>();

        // En H2: TO_CHAR puede funcionar si estás en modo Oracle, si no: FORMATDATETIME(FECHA_APERTURA_EXPEDIENTE,'dd/MM/yyyy')
        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "FORMATDATETIME(FECHA_APERTURA_EXPEDIENTE, 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE " +
              "FROM V3_TR_INDIVIDUALJL " +
              "WHERE indole_trabajo = 2 AND prestacion_fp = 2 AND arrendam_trab = 2 AND capacitacion = 2 AND antiguedad = 2 AND " +
              "prima_antiguedad = 2 AND convenio_trab = 2 AND designacion_trab_falle = 2 AND designacion_trab_act_delic = 2 AND " +
              "terminacion_lab = 2 AND recuperacion_carga = 2 AND gastos_traslados = 2 AND indemnizacion = 2 AND pago_indemnizacion = 2 AND " +
              "desacuerdo_medicos = 2 AND cobro_prestaciones = 2 AND conf_seguro_social = 2 AND otro_conf = 2";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
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
