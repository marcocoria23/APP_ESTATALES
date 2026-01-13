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
public class V3Ejecucion {

    private static final Logger LOG = Logger.getLogger(V3Ejecucion.class.getName());

    String sql;
    ArrayList<String[]> Array;

    // Estatus del expediente no debe de ser 9 = No_identificado (sin filtros)
    public ArrayList Estatus_expedienteNi() {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, "
            + "CASE WHEN ESTATUS_EXPE = 9 THEN 'No_identificado' ELSE NULL END AS ESTATUS_EXPE, "
            + "REPLACE(COMENTARIOS, ',', '') AS COMENTARIOS, "
            + "PERIODO "
            + "FROM V3_TR_EJECUCIONJL "
            + "WHERE ESTATUS_EXPE = 9";

        //System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("ESTATUS_EXPE"),
                    rs.getString("COMENTARIOS")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Estatus_expedienteNi()", ex);
        }

        return Array;
    }

    // Si ESTATUS_EXPE = 2, no debe traer FECHA_CONCLUSION o FASE_CONCLUSION (sin filtros)
    public ArrayList FaseConclucion() {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, ESTATUS_EXPE "
            + "FROM V3_TR_EJECUCIONJL "
            + "WHERE ESTATUS_EXPE = 2 "
            + "  AND (FECHA_CONCLUSION IS NOT NULL OR FASE_CONCLUSION IS NOT NULL)";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("ESTATUS_EXPE")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en FaseConclucion()", ex);
        }

        return Array;
    }
}
