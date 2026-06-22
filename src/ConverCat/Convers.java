/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package ConverCat;

import Pantallas_laborales.PMenu;
import QuerysH2.Execute;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 *
 * @author ANTONIO.CORIA
 * 
 * 
 * 
 */

  
public class Convers {

    DateTimeFormatter F_DMY = DateTimeFormatter.ofPattern("dd/MM/yyyy");
    DateTimeFormatter F_ISO = DateTimeFormatter.ISO_LOCAL_DATE;
    String sql = "", id = "";
    Execute ex=new Execute();

    public String toH2Date(String s, String Campo) {
        try {
            if (s == null) {
                return null;
            }
            s = s.trim();
            if (s.isEmpty() || s.equalsIgnoreCase("NULL")) {
                return null;
            }
            s = s.replace('-', '/');
            LocalDate d = LocalDate.parse(s, F_DMY);
            return d.format(F_ISO);
        } catch (Exception e) {
            return "ERROR EN FORMATO FECHA CAMPO:||" + Campo;
        }
    }

    public static boolean esNumero(String s) {
        if (s == null) {
            return false;
        }
        s = s.trim();
        if (s.isEmpty()) {
            return false;
        }

        try {
            Integer.parseInt(s);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public String CON_V3_TC_AUD_TIPO_PROCEJL(Connection con, String campo) {
        if (campo == null || campo.trim().isEmpty()) {
            return null;
         }else {
            if (!esNumero(campo)) {
                String sql = "SELECT ID FROM V3_TC_AUD_TIPO_PROCEJL "
                        + "WHERE UPPER(TRIM(DESCRIPCION)) = ?";

                try ( PreparedStatement ps = con.prepareStatement(sql)) {
                    ps.setString(1, campo.toUpperCase().trim());

                    try ( ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString("ID");
                        } else {
                            return "-404";
                       }
                    }

                } catch (SQLException e) {
                    System.err.println("Error en CON_V3_TC_AUD_TIPO_PROCEJL");
                    e.printStackTrace();
                    return "Error SQL";
                }
            } else {
                return campo;
            }
        }
    }
   
    
    public String CON_V3_EXPEDIENTE(String campo) {
        if (!campo.trim().equals("")) {
            if (campo.startsWith("ENE") || campo.startsWith("FEB") || campo.startsWith("MAR") || campo.startsWith("ABR") || campo.startsWith("MAY") || campo.startsWith("JUN") || campo.startsWith("JUL") || campo.startsWith("AGO") || campo.startsWith("SEP") || campo.startsWith("OCT") || campo.startsWith("NOV") || campo.startsWith("DIC")) {
                String[] parts;
                parts = campo.split("-");
                String partAño = "", partMes = "", AñoDef = "";
                partMes = parts[0];
                partAño = parts[1];
                if (partMes.trim().equals("ENE")) {
                    return "01/" + "20" + partAño;
                }
                if (partMes.trim().equals("FEB")) {
                    return "02/" + "20" + partAño;
                }
                if (partMes.trim().equals("MAR")) {
                    return "03/" + "20" + partAño;
                }
                if (partMes.trim().equals("ABR")) {
                    return "04/" + "20" + partAño;
                }
                if (partMes.trim().equals("MAY")) {
                    return "05/" + "20" + partAño;
                }
                if (partMes.trim().equals("JUN")) {
                    return "06/" + "20" + partAño;
                }
                if (partMes.trim().equals("JUL")) {
                    return "07/" + "20" + partAño;
                }
                if (partMes.trim().equals("AGO")) {
                    return "08/" + "20" + partAño;
                }
                if (partMes.trim().equals("SEP")) {
                    return "09/" + "20" + partAño;
                }
                if (partMes.trim().equals("OCT")) {
                    return "10/" + "20" + partAño;
                }
                if (partMes.trim().equals("NOV")) {
                    return "11/" + "20" + partAño;
                }
                if (partMes.trim().equals("DIC")) {
                    return "12/" + "20" + partAño;
                }
            } 
        }
        return campo;
    }
    
    

    public String CON_V3_TC_AUD_TIPO_AUDIENJL(Connection con, String campo) {
        if (campo == null || campo.trim().isEmpty()) {
            return null;
        } else {
            if (campo.toUpperCase().trim().equals("AUDIENCIA DENTRO DEL PROCEDIMIENTO COLECTIVO DE NATURALEZA ECONOMICA")){
                campo="5";
            }
            if (!esNumero(campo)) {
                String sql = "SELECT ID FROM V3_TC_AUD_TIPO_AUDIENJL "
                        + "WHERE UPPER(TRIM(DESCRIPCION)) = ?";

                try ( PreparedStatement ps = con.prepareStatement(sql)) {
                    ps.setString(1, campo.toUpperCase().trim());

                    try ( ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString("ID");
                        } else {
                            return "-404";
                        }
                    }

                } catch (SQLException e) {
                    System.err.println("Error en CON_V3_TC_AUD_TIPO_AUDIENJL");
                    e.printStackTrace();
                    return "Error SQL";
                }
            } else {
                return campo;
            }
        }
    }

    public String CON_V3_TC_ACTORJL(Connection con, String campo) {
        if (campo == null || campo.trim().isEmpty()) {
            return null;
        } else {
            if (!esNumero(campo)) {
                String sql = "SELECT ID FROM V3_TC_ACTORJL "
                        + "WHERE UPPER(TRIM(DESCRIPCION)) = ?";

                try ( PreparedStatement ps = con.prepareStatement(sql)) {
                    ps.setString(1, campo.toUpperCase().trim());

                    try ( ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString("ID");
                        } else {
                            return "-404";
                        }
                    }

                } catch (SQLException e) {
                    System.err.println("Error en CON_V3_TC_ACTORJL");
                    e.printStackTrace();
                    return "Error SQL";
                }
            } else {
                return campo;
            }
        }
    }

    public String CON_V3_TC_CAU_IMPI_ADMI_DEMJL(Connection con, String campo) {
        if (campo == null || campo.trim().isEmpty()) {
            return null;
        } else {
            if (!esNumero(campo)) {
                String sql = "SELECT ID FROM V3_TC_CAU_IMPI_ADMI_DEMJL "
                        + "WHERE UPPER(TRIM(DESCRIPCION)) = ?";

                try ( PreparedStatement ps = con.prepareStatement(sql)) {
                    ps.setString(1, campo.toUpperCase().trim());

                    try ( ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString("ID");
                        } else {
                            return "-404";
                        }
                    }

                } catch (SQLException e) {
                    System.err.println("Error en CON_V3_TC_CAU_IMPI_ADMI_DEMJL");
                    e.printStackTrace();
                    return "Error SQL";
                }
            } else {
                return campo;
            }
        }
    }

    public String CON_V3_TC_CIRCUNS_ORGANOJL(Connection con, String campo) {
        if (campo == null || campo.trim().isEmpty()) {
            return null;
        } else {
            if (!esNumero(campo)) {
                String sql = "SELECT ID FROM V3_TC_CIRCUNS_ORGANOJL "
                        + "WHERE UPPER(TRIM(DESCRIPCION)) = ?";

                try ( PreparedStatement ps = con.prepareStatement(sql)) {
                    ps.setString(1, campo.toUpperCase().trim());

                    try ( ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString("ID");
                        } else {
                            return "-404";
                        }
                    }

                } catch (SQLException e) {
                    System.err.println("Error en CON_V3_TC_CIRCUNS_ORGANOJL");
                    e.printStackTrace();
                    return "Error SQL";
                }
            } else {
                return campo;
            }
        }
    }

    public String CON_V3_TC_DEMANDADOJL(Connection con, String campo) {
        if (campo == null || campo.trim().isEmpty()) {
            return null;
        } else {
            if (!esNumero(campo)) {
                String sql = "SELECT ID FROM V3_TC_DEMANDADOJL "
                        + "WHERE UPPER(TRIM(DESCRIPCION)) = ?";

                try ( PreparedStatement ps = con.prepareStatement(sql)) {
                    ps.setString(1, campo.toUpperCase().trim());

                    try ( ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString("ID");
                        } else {
                            return "-404";
                        }
                    }

                } catch (SQLException e) {
                    System.err.println("Error en CON_V3_TC_DEMANDADOJL");
                    e.printStackTrace();
                    return "Error SQL";
                }
            } else {
                return campo;
            }
        }
    }

    public String CON_V3_TC_EDAD_TRABAJADORJL(Connection con, String campo) {
        if (campo == null || campo.trim().isEmpty()) {
            return null;
        } else {
              int IdEntidad=ex.EntidadInicio(con);
            if (IdEntidad==4 || IdEntidad==5||IdEntidad==6||IdEntidad==7||IdEntidad==8||IdEntidad==10||IdEntidad==11||IdEntidad==12
                    ||IdEntidad==13||IdEntidad==14||IdEntidad==17||IdEntidad==18||IdEntidad==20||IdEntidad==21||IdEntidad==22||IdEntidad==23
                    ||IdEntidad==27||IdEntidad==29||IdEntidad==30||IdEntidad==32) {
                String sql = "SELECT ID FROM V3_TC_EDAD_TRABAJADORJL "
                        + "WHERE UPPER(TRIM(DESCRIPCION)) = ?";

                try ( PreparedStatement ps = con.prepareStatement(sql)) {
                    ps.setString(1, campo.toUpperCase().trim());

                    try ( ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString("ID");
                        } else {
                            return "-404";
                        }
                    }

                } catch (SQLException e) {
                    System.err.println("Error en CON_V3_TC_EDAD_TRABAJADORJL");
                    e.printStackTrace();
                    return "Error SQL";
                }
            } else {
                return campo;
            }
        }
    }

    public String CON_V3_TC_ENTIDADESJL(Connection con, String campo) {
        if (campo == null || campo.trim().isEmpty()) {
            return null;
        } else {
            if (!esNumero(campo)) {
                String sql = "SELECT ENTIDAD_ID FROM V3_TC_ENTIDADESJL "
                        + "WHERE UPPER(TRIM(DESCRIPCION)) = ?";

                try ( PreparedStatement ps = con.prepareStatement(sql)) {
                    ps.setString(1, campo.toUpperCase().trim());

                    try ( ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString("ENTIDAD_ID");
                        } else {
                            return "-404";
                        }
                    }

                } catch (SQLException e) {
                    System.err.println("Error en CON_V3_TC_ENTIDADESJL");
                    e.printStackTrace();
                    return "Error SQL";
                }
            } else {
                return campo;
            }
        }
    }

    public String CON_V3_TC_ESTATUS_DEMANDAJL(Connection con, String campo) {
        if (campo == null || campo.trim().isEmpty()) {
            return null;
        } else {
            if (!esNumero(campo)) {
                String sql = "SELECT ID FROM V3_TC_ESTATUS_DEMANDAJL "
                        + "WHERE UPPER(TRIM(DESCRIPCION)) = ?";

                try ( PreparedStatement ps = con.prepareStatement(sql)) {
                    ps.setString(1, campo.toUpperCase().trim());

                    try ( ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString("ID");
                        } else {
                            return "-404";
                        }
                    }

                } catch (SQLException e) {
                    System.err.println("Error en CON_V3_TC_ESTATUS_DEMANDAJL");
                    e.printStackTrace();
                    return "Error SQL";
                }
            } else {
                return campo;
            }
        }
    }

    public String CON_V3_TC_ESTATUS_EXPEDIENTEJL(Connection con, String campo) {
        if (campo == null || campo.trim().isEmpty()) {
            return null;
        } else {
            if (!esNumero(campo)) {
                String sql = "SELECT ID FROM V3_TC_ESTATUS_EXPEDIENTEJL "
                        + "WHERE UPPER(TRIM(DESCRIPCION)) = ?";

                try ( PreparedStatement ps = con.prepareStatement(sql)) {
                    ps.setString(1, campo.toUpperCase().trim());

                    try ( ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString("ID");
                        } else {
                            return "-404";
                        }
                    }

                } catch (SQLException e) {
                    System.err.println("Error en CON_V3_TC_ESTATUS_EXPEDIENTEJL");
                    e.printStackTrace();
                    return "Error SQL";
                }
            } else {
                return campo;
            }
        }
    }

    public String CON_V3_TC_EXISTENCIA_HUELGAJL(Connection con, String campo) {
        if (campo == null || campo.trim().isEmpty()) {
            return null;
        } else {
            if (!esNumero(campo)) {
                String sql = "SELECT ID FROM V3_TC_EXISTENCIA_HUELGAJL "
                        + "WHERE UPPER(TRIM(DESCRIPCION)) = ?";

                try ( PreparedStatement ps = con.prepareStatement(sql)) {
                    ps.setString(1, campo.toUpperCase().trim());

                    try ( ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString("ID");
                        } else {
                            return "-404";
                        }
                    }

                } catch (SQLException e) {
                    System.err.println("Error en CON_V3_TC_EXISTENCIA_HUELGAJL");
                    e.printStackTrace();
                    return "Error SQL";
                }
            } else {
                return campo;
            }
        }
    }

    public String CON_V3_TC_FASE_CONCLUSION_EJEJL(Connection con, String campo) {
        if (campo == null || campo.trim().isEmpty()) {
            return null;
        } else {
            if (!esNumero(campo)) {
                String sql = "SELECT ID FROM V3_TC_FASE_CONCLUSION_EJEJL "
                        + "WHERE UPPER(TRIM(DESCRIPCION)) = ?";

                try ( PreparedStatement ps = con.prepareStatement(sql)) {
                    ps.setString(1, campo.toUpperCase().trim());

                    try ( ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString("ID");
                        } else {
                            return "-404";
                        }
                    }

                } catch (SQLException e) {
                    System.err.println("Error en CON_V3_TC_FASE_CONCLUSION_EJEJL");
                    e.printStackTrace();
                    return "Error SQL";
                }
            } else {
                return campo;
            }
        }
    }

    public String CON_V3_TC_FASE_EXPEDIENTEJL(Connection con, String campo) {
        if (campo == null || campo.trim().isEmpty()) {
            return null;
        } else {
            if (!esNumero(campo)) {
                String sql = "SELECT ID FROM V3_TC_FASE_EXPEDIENTEJL "
                        + "WHERE UPPER(TRIM(DESCRIPCION)) = ?";

                try ( PreparedStatement ps = con.prepareStatement(sql)) {
                    ps.setString(1, campo.toUpperCase().trim());

                    try ( ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString("ID");
                        } else {
                            return "-404";
                        }
                    }

                } catch (SQLException e) {
                    System.err.println("Error en CON_V3_TC_FASE_EXPEDIENTEJL");
                    e.printStackTrace();
                    return "Error SQL";
                }
            } else {
                return campo;
            }
        }
    }

    public String CON_V3_TC_FORMA_SOLUCION_HJL(Connection con, String campo) {
        if (campo == null || campo.trim().isEmpty()) {
            return null;
        } else {
            if (!esNumero(campo)) {
                String sql = "SELECT ID FROM V3_TC_FORMA_SOLUCION_HJL "
                        + "WHERE UPPER(TRIM(DESCRIPCION)) = ?";

                try ( PreparedStatement ps = con.prepareStatement(sql)) {
                    ps.setString(1, campo.toUpperCase().trim());

                    try ( ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString("ID");
                        } else {
                            return "-404";
                        }
                    }

                } catch (SQLException e) {
                    System.err.println("Error en CON_V3_TC_FORMA_SOLUCION_HJL");
                    e.printStackTrace();
                    return "Error SQL";
                }
            } else {
                return campo;
            }
        }
    }

    public String CON_V3_TC_FORMA_SOLUCION_PHJL(Connection con, String campo) {
        if (campo == null || campo.trim().isEmpty()) {
            return null;
        } else {
            if (!esNumero(campo)) {
                String sql = "SELECT ID FROM V3_TC_FORMA_SOLUCION_PHJL "
                        + "WHERE UPPER(TRIM(DESCRIPCION)) = ?";

                try ( PreparedStatement ps = con.prepareStatement(sql)) {
                    ps.setString(1, campo.toUpperCase().trim());

                    try ( ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString("ID");
                        } else {
                            return "-404";
                        }
                    }

                } catch (SQLException e) {
                    System.err.println("Error en CON_V3_TC_FORMA_SOLUCION_PHJL");
                    e.printStackTrace();
                    return "Error SQL";
                }
            } else {
                return campo;
            }
        }
    }

    public String CON_V3_TC_FORMA_SOLUCIONJL(Connection con, String campo) {
        if (campo == null || campo.trim().isEmpty()) {
            return null;
        } else {
            if (!esNumero(campo)) {
                String sql = "SELECT ID FROM V3_TC_FORMA_SOLUCIONJL "
                        + "WHERE UPPER(TRIM(DESCRIPCION)) = ?";

                try ( PreparedStatement ps = con.prepareStatement(sql)) {
                    ps.setString(1, campo.toUpperCase().trim());

                    try ( ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString("ID");
                        } else {
                            return "-404";
                        }
                    }

                } catch (SQLException e) {
                    System.err.println("Error en CON_V3_TC_FORMA_SOLUCIONJL");
                    e.printStackTrace();
                    return "Error SQL";
                }
            } else {
                return campo;
            }
        }
    }

    public String CON_V3_TC_JORNADA_TRABAJADORJL(Connection con, String campo) {
        if (campo == null || campo.trim().isEmpty()) {
            return null;
        } else {
            if (!esNumero(campo)) {
                String sql = "SELECT ID FROM V3_TC_JORNADA_TRABAJADORJL "
                        + "WHERE UPPER(TRIM(DESCRIPCION)) = ?";

                try ( PreparedStatement ps = con.prepareStatement(sql)) {
                    ps.setString(1, campo.toUpperCase().trim());

                    try ( ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString("ID");
                        } else {
                            return "-404";
                        }
                    }

                } catch (SQLException e) {
                    System.err.println("Error en CON_V3_TC_JORNADA_TRABAJADORJL");
                    e.printStackTrace();
                    return "Error SQL";
                }
            } else {
                return campo;
            }
        }
    }

    public String CON_V3_TC_JURISDICCIONJL(Connection con, String campo) {
        if (campo == null || campo.trim().isEmpty()) {
            return null;
        } else {
            if (!esNumero(campo)) {
                String sql = "SELECT ID FROM V3_TC_JURISDICCIONJL "
                        + "WHERE UPPER(TRIM(DESCRIPCION)) = ?";

                try ( PreparedStatement ps = con.prepareStatement(sql)) {
                    ps.setString(1, campo.toUpperCase().trim());

                    try ( ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString("ID");
                        } else {
                            return "-404";
                        }
                    }

                } catch (SQLException e) {
                    System.err.println("Error en CON_V3_TC_JURISDICCIONJL");
                    e.printStackTrace();
                    return "Error SQL";
                }
            } else {
                return campo;
            }
        }
    }

    public String CON_V3_TC_LICITUD_HUELGAJL(Connection con, String campo) {
        if (campo == null || campo.trim().isEmpty()) {
            return null;
        } else {
            if (!esNumero(campo)) {
                String sql = "SELECT ID FROM V3_TC_LICITUD_HUELGAJL "
                        + "WHERE UPPER(TRIM(DESCRIPCION)) = ?";

                try ( PreparedStatement ps = con.prepareStatement(sql)) {
                    ps.setString(1, campo.toUpperCase().trim());

                    try ( ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString("ID");
                        } else {
                            return "-404";
                        }
                    }

                } catch (SQLException e) {
                    System.err.println("Error en CON_V3_TC_LICITUD_HUELGAJL");
                    e.printStackTrace();
                    return "Error SQL";
                }
            } else {
                return campo;
            }
        }
    }

    public String CON_V3_TC_MOTIVO_PROMOCIONJL(Connection con, String campo) {
        if (campo == null || campo.trim().isEmpty()) {
            return null;
        }else{ 
       if (campo.toUpperCase().trim().equals("INCUMPLIMIENTO DE CONVENIO CELEBRADO ANTE EL CENTRO FEDERAL DE CONCILIACIÓN Y REGISTRO LABORAL")) {
           return "2";
       }else {
            if (!esNumero(campo)) {
                String sql = "SELECT ID FROM V3_TC_MOTIVO_PROMOCIONJL "
                        + "WHERE UPPER(TRIM(DESCRIPCION)) = ?";

                try ( PreparedStatement ps = con.prepareStatement(sql)) {
                    ps.setString(1, campo.toUpperCase().trim());

                    try ( ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString("ID");
                        } else {
                            return "-404";
                        }
                    }

                } catch (SQLException e) {
                    System.err.println("Error en CON_V3_TC_MOTIVO_PROMOCIONJL");
                    e.printStackTrace();
                    return "Error SQL";
                }
            } else {
                return campo;
            }
        }
    }
    }

    public String CON_V3_TC_MOTIVO_SOLICITUDJL(Connection con, String campo) {
        if (campo == null || campo.trim().isEmpty()) {
            return null;
        } else {
            if (!esNumero(campo)) {
                String sql = "SELECT ID FROM V3_TC_MOTIVO_SOLICITUDJL "
                        + "WHERE UPPER(TRIM(DESCRIPCION)) = ?";

                try ( PreparedStatement ps = con.prepareStatement(sql)) {
                    ps.setString(1, campo.toUpperCase().trim());

                    try ( ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString("ID");
                        } else {
                            return "-404";
                        }
                    }

                } catch (SQLException e) {
                    System.err.println("Error en CON_V3_TC_MOTIVO_SOLICITUDJL");
                    e.printStackTrace();
                    return "Error SQL";
                }
            } else {
                return campo;
            }
        }
    }

    public String CON_V3_TC_MUNICIPIOJL(Connection con, String campo, int entidadId) {
        if (campo == null || campo.trim().isEmpty()) {
            return null;
        } else {
            if (!esNumero(campo)) {
                String sql = "SELECT MUNICIPIO_ID FROM V3_TC_MUNICIPIOJL "
                        + "WHERE ENTIDAD_ID = ? AND UPPER(TRIM(DESCRIPCION)) = ?";

                try ( PreparedStatement ps = con.prepareStatement(sql)) {
                    ps.setInt(1, entidadId);
                    ps.setString(2, campo.toUpperCase().trim());

                    try ( ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString("MUNICIPIO_ID");
                        } else {
                            return "-404";
                        }
                    }

                } catch (SQLException e) {
                    System.err.println("Error en CON_V3_TC_MUNICIPIOJL");
                    e.printStackTrace();
                    return "Error SQL";
                }
            } else {
                return campo;
            }
        }
    }

    public String CON_V3_TC_NAT_CONFLICTOJL(Connection con, String campo) {
        if (campo == null || campo.trim().isEmpty()) {
            return null;
        } else {
            if (!esNumero(campo)) {
                String sql = "SELECT ID FROM V3_TC_NAT_CONFLICTOJL "
                        + "WHERE UPPER(TRIM(DESCRIPCION)) = ?";

                try ( PreparedStatement ps = con.prepareStatement(sql)) {
                    ps.setString(1, campo.toUpperCase().trim());

                    try ( ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString("ID");
                        } else {
                            return "-404";
                        }
                    }

                } catch (SQLException e) {
                    System.err.println("Error en CON_V3_TC_NAT_CONFLICTOJL");
                    e.printStackTrace();
                    return "Error SQL";
                }
            } else {
                return campo;
            }
        }
    }

    public String CON_V3_TC_OCUPACION_TRABAJADORJL(Connection con, String campo) {
    // 1) Nulos / vacíos
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    // Normalización (una sola vez)
    String c = campo.trim().toUpperCase().replaceAll("\\n", "");
    // 2) Casos especiales
    if (c.equals("NO IDENTIFICADO")
            || c.equals("NO IDENTIFICADA")
            || c.equals("OCUPACIONES NO IDENTIFICADO")
            || c.equals("OCUPACIONES NO DEFINIDAS")) {
        return "999";
    }
    // Nota: aquí arreglé "PRODUCTOSDE" -> "PRODUCTOS DE" (si en tu BD viene pegado, déjalo igual)
    if (c.equals("SUPERVISORES DE TRABAJADORES EN LA ELABORACIÓN Y PROCESAMIENTO DE ALIMENTOS, BEBIDAS Y PRODUCTOSDE TABACO")) {
        return "365";
    }
    if (c.equals("SASTRES Y MODISTOS, COSTURERAS Y CONFECCIONADORES DE PRENDAS Y ACCESORIOS DE VESTIR, DE TELA, CUERO,PIEL Y SIMILARES")) {
        return "354";
    }
    // 3) Si NO es número, buscar ID por descripción
    if (!esNumero(campo)) {
        String sql = "SELECT ID FROM V3_TC_OCUPACION_TRABAJADORJL "
                   + "WHERE UPPER(TRIM(DESCRIPCION)) = ?";

        try (PreparedStatement ps = con.prepareStatement(sql)) {
            ps.setString(1, c);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("ID");
                } else {
                    return "-404"; // no encontrado
                }
            }
        } catch (SQLException e) {
            System.err.println("Error en CON_V3_TC_OCUPACION_TRABAJADORJL");
            e.printStackTrace();
            return "Error SQL";
        }
    }
    // 4) Si ya es número, devolver tal cual (puedes devolver campo.trim() si quieres)
    return campo;
}


    public String CON_V3_TC_ORGAN_OBRERAJL(Connection con, String campo) {
        if (campo == null || campo.trim().isEmpty()) {
            return null;
        } else {
            if (!esNumero(campo)) {
                String sql = "SELECT ID FROM V3_TC_ORGAN_OBRERAJL "
                        + "WHERE UPPER(TRIM(DESCRIPCION)) = ?";

                try ( PreparedStatement ps = con.prepareStatement(sql)) {
                    ps.setString(1, campo.toUpperCase().trim());

                    try ( ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString("ID");
                        } else {
                            return "-404";
                        }
                    }

                } catch (SQLException e) {
                    System.err.println("Error en CON_V3_TC_ORGAN_OBRERAJL");
                    e.printStackTrace();
                    return "Error SQL";
                }
            } else {
                return campo;
            }
        }
    }

    public String CON_V3_TC_PROMOVENTEJL(Connection con, String campo) {
        if (campo == null || campo.trim().isEmpty()) {
            return null;
        } else {
            if (!esNumero(campo)) {
                String sql = "SELECT ID FROM V3_TC_PROMOVENTEJL "
                        + "WHERE UPPER(TRIM(DESCRIPCION)) = ?";

                try ( PreparedStatement ps = con.prepareStatement(sql)) {
                    ps.setString(1, campo.toUpperCase().trim());

                    try ( ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString("ID");
                        } else {
                            return "-404";
                        }
                    }

                } catch (SQLException e) {
                    System.err.println("Error en CON_V3_TC_PROMOVENTEJL");
                    e.printStackTrace();
                    return "Error SQL";
                }
            } else {
                return campo;
            }
        }
    }

    public String CON_V3_TC_RESPUESTA_SIMPLEJL(Connection con, String campo) {
        if (campo == null || campo.trim().isEmpty()) {
            return null;
        } else {
            if (campo.trim().toUpperCase().equals("SI"))
            {
             return "1";   
            }else{
            if (!esNumero(campo)) {
                String sql = "SELECT ID FROM V3_TC_RESPUESTA_SIMPLEJL "
                        + "WHERE UPPER(TRIM(DESCRIPCION)) = ?";

                try ( PreparedStatement ps = con.prepareStatement(sql)) {
                    ps.setString(1, campo.toUpperCase().trim());

                    try ( ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString("ID");
                        } else {
                            return "-404";
                        }
                    }

                } catch (SQLException e) {
                    System.err.println("Error en CON_V3_TC_RESPUESTA_SIMPLEJL");
                    e.printStackTrace();
                    return "Error SQL";
                }
            } else {
                return campo;
            }
        }
    }
   }

    public String CON_V3_TC_SECTOR_RAMAJL(Connection con, String campo) {
       // System.out.println("camposector"+campo);
        if (campo == null || campo.trim().isEmpty()) {
            return null;
        } else {
            if (!esNumero(campo)) {
                String sql = "SELECT ID FROM V3_TC_SECTOR_RAMAJL "
                        + "WHERE REPLACE(REPLACE(UPPER(TRIM(DESCRIPCION)),'_',' '),',','') = ?";

                try ( PreparedStatement ps = con.prepareStatement(sql)) {
                    ps.setString(1, campo.toUpperCase().trim().replace("_", " ").replace(",", ""));

                    try ( ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString("ID");
                        } else {
                            return "-404";
                        }
                    }

                } catch (SQLException e) {
                    System.err.println("Error en CON_V3_TC_SECTOR_RAMAJL");
                    e.printStackTrace();
                    return "Error SQL";
                }
            } else {
                return campo;
            }
        }
    }

    public String CON_V3_TC_SENTE_INCIDENTALJL(Connection con, String campo) {
        if (campo == null || campo.trim().isEmpty()) {
            return null;
        } else {
            if (!esNumero(campo)) {
                String sql = "SELECT ID FROM V3_TC_SENTE_INCIDENTALJL "
                        + "WHERE UPPER(TRIM(DESCRIPCION)) = ?";

                try ( PreparedStatement ps = con.prepareStatement(sql)) {
                    ps.setString(1, campo.toUpperCase().trim());

                    try ( ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString("ID");
                        } else {
                            return "-404";
                        }
                    }

                } catch (SQLException e) {
                    System.err.println("Error en CON_V3_TC_SENTE_INCIDENTALJL");
                    e.printStackTrace();
                    return "Error SQL";
                }
            } else {
                return campo;
            }
        }
    }

    public String CON_V3_TC_SEXO_TRABAJADORJL(Connection con, String campo) {
        if (campo == null || campo.trim().isEmpty()) {
            return null;
        } else {
            if (!esNumero(campo)) {
                String sql = "SELECT ID FROM V3_TC_SEXO_TRABAJADORJL "
                        + "WHERE UPPER(TRIM(DESCRIPCION)) = ?";

                try ( PreparedStatement ps = con.prepareStatement(sql)) {
                    ps.setString(1, campo.toUpperCase().trim());

                    try ( ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString("ID");
                        } else {
                            return "-404";
                        }
                    }

                } catch (SQLException e) {
                    System.err.println("Error en CON_V3_TC_SEXO_TRABAJADORJL");
                    e.printStackTrace();
                    return "Error SQL";
                }
            } else {
                return campo;
            }
        }
    }

    public String CON_V3_TC_SUBSECTOR_RAMAJL(Connection con, String campo) {
        //System.out.println("camposubsector"+campo);
        if (campo == null || campo.trim().isEmpty()) {
            return null;
        } else {
            if (!esNumero(campo)) {
                String sql = "SELECT ID_SUBSECTOR FROM V3_TC_SUBSECTOR_RAMAJL "
                        + "WHERE UPPER(TRIM(DESCRIPCION)) = ?";
                try ( PreparedStatement ps = con.prepareStatement(sql)) {
                    ps.setString(1, campo.toUpperCase().trim());

                    try ( ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString("ID_SUBSECTOR");
                        } else {
                            return "-404";
                        }
                    }

                } catch (SQLException e) {
                    System.err.println("Error en CON_V3_TC_SUBSECTOR_RAMAJL");
                    e.printStackTrace();
                    return "Error SQL";
                }
            } else {
                return campo;
            }
        }
    }

    public String CON_V3_TC_TIPO_ASUNTOJL(Connection con, String campo) {
        if (campo == null || campo.trim().isEmpty()) {
            return null;
        } else {
            if (!esNumero(campo)) {
                String sql = "SELECT ID FROM V3_TC_TIPO_ASUNTOJL "
                        + "WHERE UPPER(TRIM(DESCRIPCION)) = ?";

                try ( PreparedStatement ps = con.prepareStatement(sql)) {
                    ps.setString(1, campo.toUpperCase().trim());

                    try ( ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString("ID");
                        } else {
                            return "-404";
                        }
                    }

                } catch (SQLException e) {
                    System.err.println("Error en CON_V3_TC_TIPO_ASUNTOJL");
                    e.printStackTrace();
                    return "Error SQL";
                }
            } else {
                return campo;
            }
        }
    }

    public String CON_V3_TC_TIPO_CONTRATOJL(Connection con, String campo) {
        if (campo == null || campo.trim().isEmpty()) {
            return null;
        } else {
            if (!esNumero(campo)) {
                String sql = "SELECT ID FROM V3_TC_TIPO_CONTRATOJL "
                        + "WHERE UPPER(TRIM(DESCRIPCION)) = ?";

                try ( PreparedStatement ps = con.prepareStatement(sql)) {
                    ps.setString(1, campo.toUpperCase().trim());

                    try ( ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString("ID");
                        } else {
                            return "-404";
                        }
                    }

                } catch (SQLException e) {
                    System.err.println("Error en CON_V3_TC_TIPO_CONTRATOJL");
                    e.printStackTrace();
                    return "Error SQL";
                }
            } else {
                return campo;
            }
        }
    }

    public String CON_V3_TC_TIPO_DEFENSAJL(Connection con, String campo) {
        if (campo == null || campo.trim().isEmpty()) {
            return null;
        } else {
            if (!esNumero(campo)) {
                String sql = "SELECT ID FROM V3_TC_TIPO_DEFENSAJL "
                        + "WHERE UPPER(TRIM(DESCRIPCION)) = ?";

                try ( PreparedStatement ps = con.prepareStatement(sql)) {
                    ps.setString(1, campo.toUpperCase().trim());

                    try ( ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString("ID");
                        } else {
                            return "-404";
                        }
                    }

                } catch (SQLException e) {
                    System.err.println("Error en CON_V3_TC_TIPO_DEFENSAJL");
                    e.printStackTrace();
                    return "Error SQL";
                }
            } else {
                return campo;
            }
        }
    }

    public String CON_V3_TC_TIPO_INCIDENTEJL(Connection con, String campo) {
        if (campo == null || campo.trim().isEmpty()) {
            return null;
        } else {
            if (!esNumero(campo)) {
                String sql = "SELECT ID FROM V3_TC_TIPO_INCIDENTEJL "
                        + "WHERE UPPER(TRIM(DESCRIPCION)) = ?";

                try ( PreparedStatement ps = con.prepareStatement(sql)) {
                    ps.setString(1, campo.toUpperCase().trim());

                    try ( ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString("ID");
                        } else {
                            return "-404";
                        }
                    }

                } catch (SQLException e) {
                    System.err.println("Error en CON_V3_TC_TIPO_INCIDENTEJL");
                    e.printStackTrace();
                    return "Error SQL";
                }
            } else {
                return campo;
            }
        }
    }

    public String CON_V3_TC_TIPO_INCOMPETENCIAJL(Connection con, String campo) {
        if (campo == null || campo.trim().isEmpty()) {
            return null;
        } else {
            if (!esNumero(campo)) {
                String sql = "SELECT ID FROM V3_TC_TIPO_INCOMPETENCIAJL "
                        + "WHERE UPPER(TRIM(DESCRIPCION)) = ?";

                try ( PreparedStatement ps = con.prepareStatement(sql)) {
                    ps.setString(1, campo.toUpperCase().trim());

                    try ( ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString("ID");
                        } else {
                            return "-404";
                        }
                    }

                } catch (SQLException e) {
                    System.err.println("Error en CON_V3_TC_TIPO_INCOMPETENCIAJL");
                    e.printStackTrace();
                    return "Error SQL";
                }
            } else {
                return campo;
            }
        }
    }

    public String CON_V3_TC_TIPO_PATRONJL(Connection con, String campo) {
        if (campo == null || campo.trim().isEmpty()) {
            return null;
        } else {
            if (!esNumero(campo)) {
                String sql = "SELECT ID FROM V3_TC_TIPO_PATRONJL "
                        + "WHERE UPPER(TRIM(DESCRIPCION)) = ?";

                try ( PreparedStatement ps = con.prepareStatement(sql)) {
                    ps.setString(1, campo.toUpperCase().trim());

                    try ( ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString("ID");
                        } else {
                            return "-404";
                        }
                    }

                } catch (SQLException e) {
                    System.err.println("Error en CON_V3_TC_TIPO_PATRONJL");
                    e.printStackTrace();
                    return "Error SQL";
                }
            } else {
                return campo;
            }
        }
    }

    public String CON_V3_TC_TIPO_SENTENCIAJL(Connection con, String campo) {
        if (campo == null || campo.trim().isEmpty()) {
            return null;
        } else {
            if (!esNumero(campo)) {
                String sql = "SELECT ID FROM V3_TC_TIPO_SENTENCIAJL "
                        + "WHERE UPPER(TRIM(DESCRIPCION)) = ?";

                try ( PreparedStatement ps = con.prepareStatement(sql)) {
                    ps.setString(1, campo.toUpperCase().trim());

                    try ( ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString("ID");
                        } else {
                            return "-404";
                        }
                    }

                } catch (SQLException e) {
                    System.err.println("Error en CON_V3_TC_TIPO_SENTENCIAJL");
                    e.printStackTrace();
                    return "Error SQL";
                }
            } else {
                return campo;
            }
        }
    }

    public String CON_V3_TC_TIPO_SINDICATOJL(Connection con, String campo) {
        if (campo == null || campo.trim().isEmpty()) {
            return null;
        } else {
            if (!esNumero(campo)) {
                String sql = "SELECT ID FROM V3_TC_TIPO_SINDICATOJL "
                        + "WHERE UPPER(TRIM(DESCRIPCION)) = ?";

                try ( PreparedStatement ps = con.prepareStatement(sql)) {
                    ps.setString(1, campo.toUpperCase().trim());

                    try ( ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString("ID");
                        } else {
                            return "-404";
                        }
                    }

                } catch (SQLException e) {
                    System.err.println("Error en CON_V3_TC_TIPO_SINDICATOJL");
                    e.printStackTrace();
                    return "Error SQL";
                }
            } else {
                return campo;
            }
        }
    }

    public String CON_V3_TC_FASE_CONCLUSIONJL(Connection con, String campo) {
        if (campo == null || campo.trim().isEmpty()) {
            return null;
        } else {
            if (!esNumero(campo)) {
                String sql = "SELECT ID FROM V3_TC_FASE_CONCLUSION_EJEJL "
                        + "WHERE UPPER(TRIM(DESCRIPCION)) = ?";

                try ( PreparedStatement ps = con.prepareStatement(sql)) {
                    ps.setString(1, campo.toUpperCase().trim());

                    try ( ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString("ID");
                        } else {
                            return "-404";
                        }
                    }

                } catch (SQLException e) {
                    System.err.println("Error en CON_V3_TC_TIPO_SINDICATOJL");
                    e.printStackTrace();
                    return "Error SQL";
                }
            } else {
                return campo;
            }
        }
    }

    public String CON_V3_TC_OCUPACI_TRABAJADORJL(Connection con, String campo) {
     // 1) Nulos / vacíos
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    // Normalización (una sola vez)
    String c = campo.trim().toUpperCase().replaceAll("\\n", "")   ;
    // 2) Casos especiales
    if (c.equals("NO IDENTIFICADO")
            || c.equals("NO IDENTIFICADA")
            || c.equals("OCUPACIONES NO IDENTIFICADO")
            || c.equals("OCUPACIONES NO DEFINIDAS")) {
        return "999";
    }
    // Nota: aquí arreglé "PRODUCTOSDE" -> "PRODUCTOS DE" (si en tu BD viene pegado, déjalo igual)
    if (c.equals("SUPERVISORES DE TRABAJADORES EN LA ELABORACIÓN Y PROCESAMIENTO DE ALIMENTOS, BEBIDAS Y PRODUCTOSDE TABACO")) {
        return "365";
    }
    
    if (c.equals("SASTRES Y MODISTOS, COSTURERAS Y CONFECCIONADORES DE PRENDAS Y ACCESORIOS DE VESTIR, DE TELA, CUERO,PIEL Y SIMILARES")) {
        return "354";
    }
    // 3) Si NO es número, buscar ID por descripción
    if (!esNumero(campo)) {
        String sql = "SELECT ID FROM V3_TC_OCUPACION_TRABAJADORJL "
                   + "WHERE UPPER(TRIM(DESCRIPCION)) = ?";

        try (PreparedStatement ps = con.prepareStatement(sql)) {
            ps.setString(1, c);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("ID");
                } else {
                    return "-404"; // no encontrado
                }
            }
        } catch (SQLException e) {
            System.err.println("Error en CON_V3_TC_OCUPACION_TRABAJADORJL");
            e.printStackTrace();
            return "Error SQL";
        }
    }
    // 4) Si ya es número, devolver tal cual (puedes devolver campo.trim() si quieres)
    return campo;
}

}
