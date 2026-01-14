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
public class V3Control_expediente {

    private static final Logger LOG = Logger.getLogger(V3Control_expediente.class.getName());

    String sql;
    ArrayList<String[]> Array;

    // Horario debe estar completo ejemplo: 08:30 a 16:30
    public ArrayList Horario(Connection con) {
        Array = new ArrayList();

        sql = "SELECT REPLACE(NOMBRE_ORGANO_JURIS, ',', '') AS NOMBRE_ORGANO_JURIS, "
            + "CLAVE_ORGANO, HORARIO, PERIODO "
            + "FROM V3_TR_CONTROL_EXPEDIENTEJL "
            + "WHERE LENGTH(TRIM(REPLACE(HORARIO, ' ', ''))) < 6";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("NOMBRE_ORGANO_JURIS"),
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("HORARIO"),
                    rs.getString("PERIODO")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Horario()", ex);
        }
        return Array;
    }

    public ArrayList SubJuecesHom(Connection con) {
        Array = new ArrayList();

        // TO_NUMBER(TRIM(x)) -> CAST(TRIM(x) AS INT)
        sql = "SELECT CLAVE_ORGANO, "
            + "COALESCE(SUMHOM, 0) AS SUMHOM, "
            + "COALESCE(JUECES_LABORAL_SUB_HOM, 0) AS JUECES_LABORAL_SUB_HOM, "
            + "PERIODO "
            + "FROM ( "
            + "   SELECT CLAVE_ORGANO, "
            + "          (CAST(TRIM(JUECES_LABORAL_MIX_HOM) AS INT) "
            + "         + CAST(TRIM(JUECES_LABORAL_INDIV_HOM) AS INT) "
            + "         + CAST(TRIM(JUECES_LABORAL_COLEC_HOM) AS INT)) AS SUMHOM, "
            + "          CAST(TRIM(JUECES_LABORAL_SUB_HOM) AS INT) AS JUECES_LABORAL_SUB_HOM, "
            + "          PERIODO "
            + "   FROM V3_TR_CONTROL_EXPEDIENTEJL "
            + ") X "
            + "WHERE COALESCE(SUMHOM, 0) <> COALESCE(JUECES_LABORAL_SUB_HOM, 0)";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("SUMHOM"),
                    rs.getString("JUECES_LABORAL_SUB_HOM")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en SubJuecesHom()", ex);
        }
        return Array;
    }

    public ArrayList SubJuecesMuj(Connection con) {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, "
            + "COALESCE(SUMMUJ, 0) AS SUMMUJ, "
            + "COALESCE(JUECES_LABORAL_SUB_MUJ, 0) AS JUECES_LABORAL_SUB_MUJ, "
            + "PERIODO "
            + "FROM ( "
            + "   SELECT CLAVE_ORGANO, "
            + "          (CAST(TRIM(JUECES_LABORAL_MIX_MUJ) AS INT) "
            + "         + CAST(TRIM(JUECES_LABORAL_INDIV_MUJ) AS INT) "
            + "         + CAST(TRIM(JUECES_LABORAL_COLEC_MUJ) AS INT)) AS SUMMUJ, "
            + "          CAST(TRIM(JUECES_LABORAL_SUB_MUJ) AS INT) AS JUECES_LABORAL_SUB_MUJ, "
            + "          PERIODO "
            + "   FROM V3_TR_CONTROL_EXPEDIENTEJL "
            + ") X "
            + "WHERE COALESCE(SUMMUJ, 0) <> COALESCE(JUECES_LABORAL_SUB_MUJ, 0)";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("SUMMUJ"),
                    rs.getString("JUECES_LABORAL_SUB_MUJ")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en SubJuecesMuj()", ex);
        }
        return Array;
    }

    public ArrayList TotalJueces(Connection con) {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, "
            + "COALESCE(SUMHM, 0) AS SUMHM, "
            + "COALESCE(JUECES_LABORAL_TOTAL, 0) AS JUECES_LABORAL_TOTAL, "
            + "PERIODO "
            + "FROM ( "
            + "   SELECT CLAVE_ORGANO, "
            + "          (CAST(TRIM(JUECES_LABORAL_SUB_HOM) AS INT) "
            + "         + CAST(TRIM(JUECES_LABORAL_SUB_MUJ) AS INT)) AS SUMHM, "
            + "          CAST(TRIM(JUECES_LABORAL_TOTAL) AS INT) AS JUECES_LABORAL_TOTAL, "
            + "          PERIODO "
            + "   FROM V3_TR_CONTROL_EXPEDIENTEJL "
            + ") X "
            + "WHERE COALESCE(SUMHM, 0) <> COALESCE(JUECES_LABORAL_TOTAL, 0)";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("SUMHM"),
                    rs.getString("JUECES_LABORAL_TOTAL")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en TotalJueces()", ex);
        }
        return Array;
    }
}
