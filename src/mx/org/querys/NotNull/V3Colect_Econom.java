package mx.org.querys.NotNull;

import Conexion.ConexionH2;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class V3Colect_Econom {

    private static final Logger LOG = Logger.getLogger(V3Colect_Econom.class.getName());

    String sql;
    ArrayList<String[]> Array;

    ///--Tipo de asunto no debe de ser =9 No_identificado o Null.
    public ArrayList Tipo_Asunto(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "       CASE WHEN TIPO_ASUNTO = 9 THEN 'No_identificado' ELSE NULL END AS TIPO_ASUNTO " +
              "FROM V3_TR_COLECT_ECONOMJL " +
              "WHERE TIPO_ASUNTO = 9";

        try (
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
            LOG.log(Level.SEVERE, "Error en Tipo_Asunto()", ex);
        }

        return Array;
    }

    ///----cuando Suspensión temporal = No, no debe capturarse Exceso/Incosteabilidad/Falta fondos
    public ArrayList Suspencion_Temporal(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "       CASE WHEN SUSPENSION_TEMPORAL = 2 THEN 'No' ELSE NULL END AS SUSPENSION_TEMPORAL " +
              "FROM V3_TR_COLECT_ECONOMJL " +
              "WHERE SUSPENSION_TEMPORAL = 2 " +
              "  AND (EXCESO_PRODUCCION IS NOT NULL OR INCOSTEABILIDAD IS NOT NULL OR FALTA_FONDOS IS NOT NULL)";

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("SUSPENSION_TEMPORAL")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Suspencion_Temporal()", ex);
        }

        return Array;
    }

    //Estatus de la demanda no debe de ser 9=No_identificado.
    public ArrayList Estatus_demandaNI(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "       REPLACE(COMENTARIOS, ',', '') AS COMENTARIOS, " +
              "       CASE WHEN ESTATUS_DEMANDA = 9 THEN 'No_identificado' ELSE NULL END AS ESTATUS_DEMANDA " +
              "FROM V3_TR_COLECT_ECONOMJL " +
              "WHERE ESTATUS_DEMANDA = 9";

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("COMENTARIOS"),
                    rs.getString("ESTATUS_DEMANDA")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Estatus_demandaNI()", ex);
        }

        return Array;
    }

    //EN PROCESO O EN PREVENCION: estatus 5 y fecha apertura > 2 meses
    public ArrayList Estatus_Demanda_PrevenProceso(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "       CASE WHEN ESTATUS_DEMANDA = 5 THEN 'En trámite o prevención' ELSE NULL END AS ESTATUS_DEMANDA " +
              "FROM V3_TR_COLECT_ECONOMJL " +
              "WHERE ESTATUS_DEMANDA = 5 " +
              "  AND FECHA_APERTURA_EXPEDIENTE < DATEADD('MONTH', -2, CURRENT_DATE)";

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

    //Estatus del expediente no debe de ser 9=No_identificado.
    public ArrayList Estatus_expedienteNI(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "       CASE WHEN ESTATUS_EXPEDIENTE = 9 THEN 'No_identificado' ELSE NULL END AS ESTATUS_EXPEDIENTE, " +
              "       REPLACE(COMENTARIOS, ',', '') AS COMENTARIOS " +
              "FROM V3_TR_COLECT_ECONOMJL " +
              "WHERE ESTATUS_EXPEDIENTE = 9";

        try (
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
            LOG.log(Level.SEVERE, "Error en Estatus_expedienteNI()", ex);
        }

        return Array;
    }

    ///INCOMPETENCIA NO DEBE SER = 9
    public ArrayList IncompetenciaNI(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, INCOMPETENCIA, " +
              "       REPLACE(COMENTARIOS, ',', '') AS COMENTARIOS " +
              "FROM V3_TR_COLECT_ECONOMJL " +
              "WHERE INCOMPETENCIA = 9";

        try (
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
            LOG.log(Level.SEVERE, "Error en IncompetenciaNI()", ex);
        }

        return Array;
    }

    /// INCOMPETENCIA = 2 ('No') y TIPO_INCOMPETENCIA NO debería estar capturado (IS NOT NULL)
    public ArrayList Tipo_IncompetenciaNI(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "       CASE WHEN INCOMPETENCIA = 2 THEN 'No' ELSE NULL END AS INCOMPETENCIA " +
              "FROM V3_TR_COLECT_ECONOMJL " +
              "WHERE INCOMPETENCIA = 2 " +
              "  AND TIPO_INCOMPETENCIA IS NOT NULL";

        System.out.println(sql);

        try (
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
            LOG.log(Level.SEVERE, "Error en Tipo_IncompetenciaNI()", ex);
        }

        return Array;
    }

    //INCOMPETENCIA = SI (1) pero existen campos posteriores capturados (no debería)
    public ArrayList PivIncompetencia(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, INCOMPETENCIA " +
              "FROM V3_TR_COLECT_ECONOMJL " +
              "WHERE INCOMPETENCIA = 1 " +
              "  AND (FECHA_PRES_DEMANDA IS NOT NULL OR CONSTANCIA_CONS_EXPEDIDA IS NOT NULL OR CONSTANCIA_CLAVE IS NOT NULL OR " +
              "       ASUN_EXCEP_CONCILIACION IS NOT NULL OR PREVE_DEMANDA IS NOT NULL OR DESAHOGO_PREV_DEMANDA IS NOT NULL OR " +
              "       ESTATUS_DEMANDA IS NOT NULL OR FECHA_ADMISION_DEMANDA IS NOT NULL OR CANTIDAD_ACTORES IS NOT NULL OR CANTIDAD_DEMANDADOS IS NOT NULL OR " +
              "       AUDIENCIA_ECONOM IS NOT NULL OR FECHA_AUDIENCIA_ECONOM IS NOT NULL OR ESTATUS_EXPEDIENTE IS NOT NULL OR FECHA_ACTO_PROCESAL IS NOT NULL OR " +
              "       FASE_SOLI_EXPEDIENTE IS NOT NULL OR FORMA_SOLUCION IS NOT NULL OR ESPECIFIQUE_FORMA IS NOT NULL OR FECHA_RESOLUCION IS NOT NULL OR " +
              "       TIPO_SENTENCIA IS NOT NULL OR AUMENTO_PERSONAL IS NOT NULL OR DISMINUCION_PERSONAL IS NOT NULL OR AUMENTO_JORNADALAB IS NOT NULL OR " +
              "       DISMINUCION_JORNADALAB IS NOT NULL OR AUMENTO_SEMANA IS NOT NULL OR DISMINUCION_SEMANA IS NOT NULL OR AUMENTO_SALARIOS IS NOT NULL OR " +
              "       DISMINUCION_SALARIOS IS NOT NULL OR OTRO_TIPO IS NOT NULL OR ESPECIFIQUE_TIPO IS NOT NULL OR COMENTARIOS IS NOT NULL)";

        try (
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
            LOG.log(Level.SEVERE, "Error en PivIncompetencia()", ex);
        }

        return Array;
    }

    //ESTATUS DEMANDA in (2,3,4) pero hay campos posteriores capturados
    public ArrayList Estatus_Demanda_Desechada(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, ESTATUS_DEMANDA " +
              "FROM V3_TR_COLECT_ECONOMJL " +
              "WHERE ESTATUS_DEMANDA IN (2,3,4) " +
              "  AND (FECHA_ADMISION_DEMANDA IS NOT NULL OR CANTIDAD_ACTORES IS NOT NULL OR CANTIDAD_DEMANDADOS IS NOT NULL OR " +
              "       AUDIENCIA_ECONOM IS NOT NULL OR FECHA_AUDIENCIA_ECONOM IS NOT NULL OR ESTATUS_EXPEDIENTE IS NOT NULL OR " +
              "       FECHA_ACTO_PROCESAL IS NOT NULL OR FASE_SOLI_EXPEDIENTE IS NOT NULL OR FORMA_SOLUCION IS NOT NULL OR " +
              "       ESPECIFIQUE_FORMA IS NOT NULL OR FECHA_RESOLUCION IS NOT NULL OR TIPO_SENTENCIA IS NOT NULL OR " +
              "       AUMENTO_PERSONAL IS NOT NULL OR DISMINUCION_PERSONAL IS NOT NULL OR AUMENTO_JORNADALAB IS NOT NULL OR " +
              "       DISMINUCION_JORNADALAB IS NOT NULL OR AUMENTO_SEMANA IS NOT NULL OR DISMINUCION_SEMANA IS NOT NULL OR " +
              "       AUMENTO_SALARIOS IS NOT NULL OR DISMINUCION_SALARIOS IS NOT NULL OR OTRO_TIPO IS NOT NULL OR ESPECIFIQUE_TIPO IS NOT NULL)";

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

    //ESTATUS EXPEDIENTE = 2 (en proceso de resolución) pero hay campos posteriores capturados
    public ArrayList Estatus_Expediente(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, ESTATUS_EXPEDIENTE " +
              "FROM V3_TR_COLECT_ECONOMJL " +
              "WHERE ESTATUS_EXPEDIENTE = 2 " +
              "  AND (FASE_SOLI_EXPEDIENTE IS NOT NULL OR FORMA_SOLUCION IS NOT NULL OR ESPECIFIQUE_FORMA IS NOT NULL OR " +
              "       FECHA_RESOLUCION IS NOT NULL OR TIPO_SENTENCIA IS NOT NULL OR AUMENTO_PERSONAL IS NOT NULL OR " +
              "       DISMINUCION_PERSONAL IS NOT NULL OR AUMENTO_JORNADALAB IS NOT NULL OR DISMINUCION_JORNADALAB IS NOT NULL OR " +
              "       AUMENTO_SEMANA IS NOT NULL OR DISMINUCION_SEMANA IS NOT NULL OR AUMENTO_SALARIOS IS NOT NULL OR " +
              "       DISMINUCION_SALARIOS IS NOT NULL OR OTRO_TIPO IS NOT NULL OR ESPECIFIQUE_TIPO IS NOT NULL)";

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

    //cuando estatus expediente = 1 (solucionado) no debe haber fecha del último acto procesal
    public ArrayList Fecha_acto_procesal(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, ESTATUS_EXPEDIENTE, FECHA_ACTO_PROCESAL " +
              "FROM V3_TR_COLECT_ECONOMJL " +
              "WHERE ESTATUS_EXPEDIENTE = 1 " +
              "  AND FECHA_ACTO_PROCESAL IS NOT NULL";

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
        Array = new ArrayList<>();

        sql = "SELECT P.CLAVE_ORGANO, P.EXPEDIENTE_CLAVE, S.DESCRIPCION AS FASE_SOLI_EXPEDIENTE " +
              "FROM V3_TR_COLECT_ECONOMJL P " +
              "JOIN V3_TC_FASE_EXPEDIENTEJL S ON P.FASE_SOLI_EXPEDIENTE = S.ID " +
              "WHERE P.FASE_SOLI_EXPEDIENTE NOT IN (8,99)";

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

    //---CUANDO FORMA_SOLUCION = (2,3,4,5) no debe capturar info de sentencia/medidas/etc.
    public ArrayList Forma_Solucion(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, FORMA_SOLUCION " +
              "FROM V3_TR_COLECT_ECONOMJL " +
              "WHERE FORMA_SOLUCION IN (2,3,4,5) " +
              "  AND (TIPO_SENTENCIA IS NOT NULL OR AUMENTO_PERSONAL IS NOT NULL OR DISMINUCION_PERSONAL IS NOT NULL OR " +
              "       AUMENTO_JORNADALAB IS NOT NULL OR DISMINUCION_JORNADALAB IS NOT NULL OR AUMENTO_SEMANA IS NOT NULL OR " +
              "       DISMINUCION_SEMANA IS NOT NULL OR AUMENTO_SALARIOS IS NOT NULL OR DISMINUCION_SALARIOS IS NOT NULL OR " +
              "       OTRO_TIPO IS NOT NULL OR ESPECIFIQUE_TIPO IS NOT NULL)";

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FORMA_SOLUCION")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Forma_Solucion()", ex);
        }

        return Array;
    }
}
