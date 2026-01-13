package mx.org.querys.NotNull;

import Conexion.ConexionH2;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class V3_Audiencias {

    private static final Logger LOGGER = Logger.getLogger(V3_Audiencias.class.getName());

    String sql;
    ArrayList<String[]> Array;

    /// Cuando Tipo de procedimiento = Ordinario no debe capturar Especial individual, Especial colectivo, Huelga, Colectivo de naturaleza económica
    public ArrayList Aud_Ordinario() {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN TIPO_PROCED = 1 THEN 'Ordinario' ELSE CAST(TIPO_PROCED AS VARCHAR) END AS TIPO_PROCED, " +
              "PERIODO " +
              "FROM V3_TR_AUDIENCIASJL " +
              "WHERE TIPO_PROCED = 1 AND (" +
              "ESPECIAL_INDIVI_TA IS NOT NULL OR " +
              "ESPECIAL_COLECT_TA IS NOT NULL OR " +
              "HUELGA_TA IS NOT NULL OR " +
              "COL_NATU_ECONOMICA_TA IS NOT NULL" +
              ")";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("TIPO_PROCED"),
                    rs.getString("PERIODO")
                });
            }

        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Aud_Ordinario()", ex);
        }

        return Array;
    }

    /// Cuando Tipo de procedimiento = Individual no debe capturar Ordinario, Especial colectivo, Huelga, Colectivo de naturaleza económica
    public ArrayList Aud_Individual() {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN TIPO_PROCED = 2 THEN 'INDIVIDUAL' ELSE CAST(TIPO_PROCED AS VARCHAR) END AS TIPO_PROCED, " +
              "PERIODO " +
              "FROM V3_TR_AUDIENCIASJL " +
              "WHERE TIPO_PROCED = 2 AND (" +
              "ORDINARIO_TA IS NOT NULL OR " +
              "ESPECIAL_COLECT_TA IS NOT NULL OR " +
              "HUELGA_TA IS NOT NULL OR " +
              "COL_NATU_ECONOMICA_TA IS NOT NULL" +
              ")";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("TIPO_PROCED"),
                    rs.getString("PERIODO")
                });
            }

        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Aud_Individual()", ex);
        }

        return Array;
    }

    /// Cuando Tipo de procedimiento = Colectivo no debe capturar Ordinario, Especial Individual, Huelga, Colectivo de naturaleza económica
    public ArrayList Aud_Colectivo() {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN TIPO_PROCED = 3 THEN 'Colectivo' ELSE CAST(TIPO_PROCED AS VARCHAR) END AS TIPO_PROCED, " +
              "PERIODO " +
              "FROM V3_TR_AUDIENCIASJL " +
              "WHERE TIPO_PROCED = 3 AND (" +
              "ORDINARIO_TA IS NOT NULL OR " +
              "ESPECIAL_INDIVI_TA IS NOT NULL OR " +
              "HUELGA_TA IS NOT NULL OR " +
              "COL_NATU_ECONOMICA_TA IS NOT NULL" +
              ")";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("TIPO_PROCED"),
                    rs.getString("PERIODO")
                });
            }

        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Aud_Colectivo()", ex);
        }

        return Array;
    }

    /// Cuando Tipo de procedimiento = Huelga no debe capturar Ordinario, Especial Individual, Colectivo, Colectivo de naturaleza económica
    public ArrayList Aud_Huelga() {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN TIPO_PROCED = 4 THEN 'Huelga' ELSE CAST(TIPO_PROCED AS VARCHAR) END AS TIPO_PROCED, " +
              "PERIODO " +
              "FROM V3_TR_AUDIENCIASJL " +
              "WHERE TIPO_PROCED = 4 AND (" +
              "ORDINARIO_TA IS NOT NULL OR " +
              "ESPECIAL_INDIVI_TA IS NOT NULL OR " +
              "ESPECIAL_COLECT_TA IS NOT NULL OR " +
              "COL_NATU_ECONOMICA_TA IS NOT NULL" +
              ")";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("TIPO_PROCED"),
                    rs.getString("PERIODO")
                });
            }

        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Aud_Huelga()", ex);
        }

        return Array;
    }

    /// Cuando Tipo de procedimiento = Colectivo Económico no debe capturar Ordinario, Especial Individual, Colectivo, Huelga
    public ArrayList Aud_Colect_econom() {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN TIPO_PROCED = 5 THEN 'Colectivo Economico' ELSE CAST(TIPO_PROCED AS VARCHAR) END AS TIPO_PROCED, " +
              "PERIODO " +
              "FROM V3_TR_AUDIENCIASJL " +
              "WHERE TIPO_PROCED = 5 AND (" +
              "ORDINARIO_TA IS NOT NULL OR " +
              "ESPECIAL_INDIVI_TA IS NOT NULL OR " +
              "ESPECIAL_COLECT_TA IS NOT NULL OR " +
              "HUELGA_TA IS NOT NULL" +
              ")";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("TIPO_PROCED"),
                    rs.getString("PERIODO")
                });
            }

        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Aud_Colect_econom()", ex);
        }

        return Array;
    }

    // =========================
    //  VALIDACIONES "NO EXISTE"
    // =========================

    public ArrayList Aud_NE_ORDINARIO() {
        Array = new ArrayList<>();

        sql = "SELECT DISTINCT S.EXPEDIENTE_CLAVE AS EXPEDIENTE_CLAVE, S.CLAVE_ORGANO AS CLAVE_ORGANO " +
              "FROM V3_TR_AUDIENCIASJL S " +
              "LEFT JOIN V3_TR_ORDINARIOJL P " +
              "ON S.CLAVE_ORGANO = P.CLAVE_ORGANO " +
              "AND S.EXPEDIENTE_CLAVE = P.EXPEDIENTE_CLAVE " +
              "AND S.PERIODO = P.PERIODO " +
              "WHERE S.TIPO_PROCED = 1 AND P.EXPEDIENTE_CLAVE IS NULL " +
              "ORDER BY S.CLAVE_ORGANO, S.EXPEDIENTE_CLAVE";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE")
                });
            }

        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Aud_NE_ORDINARIO()", ex);
        }

        return Array;
    }

    public ArrayList Aud_NE_INDIVIDUAL() {
        Array = new ArrayList<>();

        sql = "SELECT DISTINCT S.EXPEDIENTE_CLAVE AS EXPEDIENTE_CLAVE, S.CLAVE_ORGANO AS CLAVE_ORGANO " +
              "FROM V3_TR_AUDIENCIASJL S " +
              "LEFT JOIN V3_TR_INDIVIDUALJL P " +
              "ON S.CLAVE_ORGANO = P.CLAVE_ORGANO " +
              "AND S.EXPEDIENTE_CLAVE = P.EXPEDIENTE_CLAVE " +
              "AND S.PERIODO = P.PERIODO " +
              "WHERE S.TIPO_PROCED = 2 AND P.EXPEDIENTE_CLAVE IS NULL " +
              "ORDER BY S.CLAVE_ORGANO, S.EXPEDIENTE_CLAVE";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE")
                });
            }

        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Aud_NE_INDIVIDUAL()", ex);
        }

        return Array;
    }

    public ArrayList Aud_NE_COLECTIVO() {
        Array = new ArrayList<>();

        sql = "SELECT DISTINCT S.EXPEDIENTE_CLAVE AS EXPEDIENTE_CLAVE, S.CLAVE_ORGANO AS CLAVE_ORGANO " +
              "FROM V3_TR_AUDIENCIASJL S " +
              "LEFT JOIN V3_TR_COLECTIVOJL P " +
              "ON S.CLAVE_ORGANO = P.CLAVE_ORGANO " +
              "AND S.EXPEDIENTE_CLAVE = P.EXPEDIENTE_CLAVE " +
              "AND S.PERIODO = P.PERIODO " +
              "WHERE S.TIPO_PROCED = 3 AND P.EXPEDIENTE_CLAVE IS NULL " +
              "ORDER BY S.CLAVE_ORGANO, S.EXPEDIENTE_CLAVE";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE")
                });
            }

        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Aud_NE_COLECTIVO()", ex);
        }

        return Array;
    }

    public ArrayList Aud_NE_HUELGA() {
        Array = new ArrayList<>();

        sql = "SELECT DISTINCT S.EXPEDIENTE_CLAVE AS EXPEDIENTE_CLAVE, S.CLAVE_ORGANO AS CLAVE_ORGANO " +
              "FROM V3_TR_AUDIENCIASJL S " +
              "LEFT JOIN V3_TR_HUELGAJL P " +
              "ON S.CLAVE_ORGANO = P.CLAVE_ORGANO " +
              "AND S.EXPEDIENTE_CLAVE = P.EXPEDIENTE_CLAVE " +
              "AND S.PERIODO = P.PERIODO " +
              "WHERE S.TIPO_PROCED = 4 AND P.EXPEDIENTE_CLAVE IS NULL " +
              "ORDER BY S.CLAVE_ORGANO, S.EXPEDIENTE_CLAVE";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE")
                });
            }

        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Aud_NE_HUELGA()", ex);
        }

        return Array;
    }

    public ArrayList Aud_NE_COLECT_ECONOM() {
        Array = new ArrayList<>();

        sql = "SELECT DISTINCT S.EXPEDIENTE_CLAVE AS EXPEDIENTE_CLAVE, S.CLAVE_ORGANO AS CLAVE_ORGANO " +
              "FROM V3_TR_AUDIENCIASJL S " +
              "LEFT JOIN V3_TR_COLECT_ECONOMJL P " +
              "ON S.CLAVE_ORGANO = P.CLAVE_ORGANO " +
              "AND S.EXPEDIENTE_CLAVE = P.EXPEDIENTE_CLAVE " +
              "AND S.PERIODO = P.PERIODO " +
              "WHERE S.TIPO_PROCED = 5 AND P.EXPEDIENTE_CLAVE IS NULL " +
              "ORDER BY S.CLAVE_ORGANO, S.EXPEDIENTE_CLAVE";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE")
                });
            }

        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Aud_NE_COLECT_ECONOM()", ex);
        }

        return Array;
    }
}
