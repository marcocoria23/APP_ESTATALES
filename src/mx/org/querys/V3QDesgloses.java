package mx.org.querys;

import Conexion.ConexionH2;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class V3QDesgloses {

    private Connection getCon() throws SQLException {
        return ConexionH2.getConnection();
    }

    // ------------------------------------------------------------
    // Método genérico: evita repetir el mismo código 20 veces
    // ------------------------------------------------------------
    private ArrayList<String[]> desglose(String columnaControl, String tablaDetalle, String aliasColumna) {
        ArrayList<String[]> Array = new ArrayList<>();

        // Importante: uso COALESCE para que si el control viene NULL no te regrese NULL
        // (en tus queries el nombre "Ordinario" etc. venía de la tabla control)
        String sql =
            "SELECT prin.clave_organo AS clave_organo, " +
            "       COALESCE(prin." + columnaControl + ", '0') AS " + aliasColumna + ", " +
            "       COUNT(sec.expediente_clave) AS Total_expe " +
            "FROM V3_TR_control_expedientejl prin " +
            "LEFT JOIN " + tablaDetalle + " sec " +
            "  ON prin.clave_organo = sec.clave_organo " +
            " AND prin.periodo     = sec.periodo " +
            "GROUP BY prin.clave_organo, prin." + columnaControl + " " +
            "ORDER BY prin.clave_organo";

        System.out.println(sql);

        try (Connection con = getCon();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("clave_organo"),
                    rs.getString(aliasColumna),
                    rs.getString("Total_expe")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    // ============================================================
    // PERIODO ACTUAL (sin filtros)
    // ============================================================

    // Diferencia entre el total de expedientes ordinario vs desglose en ordinario
    public ArrayList<String[]> Desglose_OrdinarioNE() {
        return desglose("Ordinario", "V3_TR_ordinariojl", "Ordinario");
    }

    // Diferencia entre el total de expedientes individual vs desglose en individual
    public ArrayList<String[]> Desglose_IndividualNE() {
        return desglose("ESPECIAL_INDIVI", "V3_TR_individualjl", "ESPECIAL_INDIVI");
    }

    // Diferencia entre el total de expedientes colectivo vs desglose en colectivo
    public ArrayList<String[]> Desglose_ColectivoNE() {
        return desglose("ESPECIAL_COLECT", "V3_TR_colectivojl", "ESPECIAL_COLECT");
    }

    // Diferencia entre el total de expedientes huelga vs desglose en huelga
    public ArrayList<String[]> Desglose_HuelgaNE() {
        return desglose("HUELGA", "V3_TR_huelgajl", "HUELGA");
    }

    // Diferencia entre el total de expedientes colectivo naturaleza económica vs desglose
    public ArrayList<String[]> Desglose_Colec_EconomNE() {
        return desglose("COL_NATU_ECONOMICA", "V3_TR_colect_economjl", "COL_NATU_ECONOMICA");
    }

    // Diferencia entre el total de expedientes paraprocesal vs desglose
    public ArrayList<String[]> Desglose_ParaprocesalNE() {
        return desglose("PARAP_VOLUNTARIO", "V3_TR_paraprocesaljl", "PARAP_VOLUNTARIO");
    }

    // Diferencia entre el total de expedientes tercerías vs desglose
    public ArrayList<String[]> Desglose_TerceriasNE() {
        return desglose("TERCERIAS", "V3_TR_terceriasjl", "TERCERIAS");
    }

    // Diferencia entre el total de expedientes preferencia crédito vs desglose
    public ArrayList<String[]> Desglose_Pref_CreditoNE() {
        return desglose("PREF_CREDITO", "V3_TR_pref_creditojl", "PREF_CREDITO");
    }

    // Diferencia entre el total de expedientes ejecución vs desglose
    public ArrayList<String[]> Desglose_EjecucionNE() {
        return desglose("EJECUCION", "V3_TR_ejecucionjl", "EJECUCION");
    }

    // ============================================================
    // PERIODO ANTERIOR (en Oracle usabas periodoAnt)
    // Aquí SIN FILTROS, entonces realmente es lo mismo que el actual.
    // Si quieres que sí sea "periodo anterior", me dices cómo lo obtienes en H2
    // y te lo dejo parametrizado.
    // ============================================================

    public ArrayList<String[]> Desglose_OrdinarioNEAnt() {
        return desglose("Ordinario", "V3_TR_ordinariojl", "Ordinario");
    }

    public ArrayList<String[]> Desglose_IndividualNEAnt() {
        return desglose("ESPECIAL_INDIVI", "V3_TR_individualjl", "ESPECIAL_INDIVI");
    }

    public ArrayList<String[]> Desglose_ColectivoNEAnt() {
        return desglose("ESPECIAL_COLECT", "V3_TR_colectivojl", "ESPECIAL_COLECT");
    }

    public ArrayList<String[]> Desglose_HuelgaNEAnt() {
        return desglose("HUELGA", "V3_TR_huelgajl", "HUELGA");
    }

    public ArrayList<String[]> Desglose_Colec_EconomNEAnt() {
        return desglose("COL_NATU_ECONOMICA", "V3_TR_colect_economjl", "COL_NATU_ECONOMICA");
    }

    public ArrayList<String[]> Desglose_ParaprocesalNEAnt() {
        return desglose("PARAP_VOLUNTARIO", "V3_TR_paraprocesaljl", "PARAP_VOLUNTARIO");
    }

    public ArrayList<String[]> Desglose_TerceriasNEAnt() {
        return desglose("TERCERIAS", "V3_TR_terceriasjl", "TERCERIAS");
    }

    public ArrayList<String[]> Desglose_Pref_CreditoNEAnt() {
        return desglose("PREF_CREDITO", "V3_TR_pref_creditojl", "PREF_CREDITO");
    }

    public ArrayList<String[]> Desglose_EjecucionNEAnt() {
        return desglose("EJECUCION", "V3_TR_ejecucionjl", "EJECUCION");
    }
}
