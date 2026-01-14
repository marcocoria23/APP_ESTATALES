/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package ConverCat;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 *
 * @author ANTONIO.CORIA
 */
public class Convers {

    DateTimeFormatter F_DMY = DateTimeFormatter.ofPattern("dd/MM/yyyy");
    DateTimeFormatter F_ISO = DateTimeFormatter.ISO_LOCAL_DATE;
    String sql="",id="";
    
   public String toH2Date(String s,String Campo) {

    try {
        if (s == null) {
            return null;
        }
        s = s.trim();
        if (s.isEmpty() || s.equalsIgnoreCase("NULL")) {
            return null;
        }
        // Normaliza separador
        s = s.replace('-', '/');
        // Intenta convertir dd/MM/yyyy
        LocalDate d = LocalDate.parse(s, F_DMY);
        // Devuelve yyyy-MM-dd (H2)
        return d.format(F_ISO);
    } catch (Exception e) {
        return "ERROR EN FORMATO FECHA CAMPO:||"+Campo;
    }
}


 public String CON_V3_TC_AUD_TIPO_PROCEJL(Connection con, String campo) throws SQLException {
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    String sql = "SELECT ID FROM V3_TC_AUD_TIPO_PROCEJL WHERE UPPER(TRIM(DESCRIPCION)) = ?";
    try ( PreparedStatement ps = con.prepareStatement(sql)) {
        ps.setString(1, campo.toUpperCase().trim());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("ID");
            }
            else
            {
            return "Valor Cat No encontrado";
            }
        }
    }
}
 
 public String CON_V3_TC_AUD_TIPO_AUDIENJL(Connection con, String campo) throws SQLException {
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    String sql = "SELECT ID FROM V3_TC_AUD_TIPO_AUDIENJL WHERE UPPER(TRIM(DESCRIPCION)) = ?";
    try ( PreparedStatement ps = con.prepareStatement(sql)) {
        ps.setString(1, campo.toUpperCase().trim());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("ID");
            }
            else
            {
            return "Valor Cat No encontrado";
            }
        }
    }
}
 
 public String CON_V3_TC_ACTORJL(Connection con, String campo) throws SQLException {
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    String sql = "SELECT ID FROM V3_TC_ACTORJL WHERE UPPER(TRIM(DESCRIPCION)) = ?";
    try ( PreparedStatement ps = con.prepareStatement(sql)) {
        ps.setString(1, campo.toUpperCase().trim());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("ID");
            }
            else
            {
            return "Valor Cat No encontrado";
            }
        }
    }
}

public String CON_V3_TC_CAU_IMPI_ADMI_DEMJL(Connection con, String campo) throws SQLException {
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    String sql = "SELECT ID FROM V3_TC_CAU_IMPI_ADMI_DEMJL WHERE UPPER(TRIM(DESCRIPCION)) = ?";
    try ( PreparedStatement ps = con.prepareStatement(sql)) {
        ps.setString(1, campo.toUpperCase().trim());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("ID");
            }
            else
            {
            return "Valor Cat No encontrado";
            }
        }
    }
}


public String CON_V3_TC_CIRCUNS_ORGANOJL(Connection con, String campo) throws SQLException {
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    String sql = "SELECT ID FROM 3_TC_CIRCUNS_ORGANOJL WHERE UPPER(TRIM(DESCRIPCION)) = ?";
    try ( PreparedStatement ps = con.prepareStatement(sql)) {
        ps.setString(1, campo.toUpperCase().trim());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("ID");
            }
            else
            {
            return "Valor Cat No encontrado";
            }
        }
    }
}


public String CON_V3_TC_DEMANDADOJL(Connection con, String campo) throws SQLException {
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    String sql = "SELECT ID FROM V3_TC_DEMANDADOJL WHERE UPPER(TRIM(DESCRIPCION)) = ?";
    try ( PreparedStatement ps = con.prepareStatement(sql)) {
        ps.setString(1, campo.toUpperCase().trim());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("ID");
            }
            else
            {
            return "Valor Cat No encontrado";
            }
        }
    }
}


public String CON_V3_TC_EDAD_TRABAJADORJL(Connection con, String campo) throws SQLException {
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    String sql = "SELECT ID FROM  V3_TC_EDAD_TRABAJADORJL WHERE UPPER(TRIM(DESCRIPCION)) = ?";
    try ( PreparedStatement ps = con.prepareStatement(sql)) {
        ps.setString(1, campo.toUpperCase().trim());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("ID");
            }
            else
            {
            return "Valor Cat No encontrado";
            }
        }
    }
}


public String CON_V3_TC_ENTIDADESJL(Connection con, String campo) throws SQLException {
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    String sql = "SELECT ENTIDAD_ID FROM V3_TC_ENTIDADESJL WHERE UPPER(TRIM(DESCRIPCION)) = ?";
    try ( PreparedStatement ps = con.prepareStatement(sql)) {
        ps.setString(1, campo.toUpperCase().trim());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("ENTIDAD_ID");
            }
            else
            {
            return "Valor Cat No encontrado";
            }
        }
    }
}


public String CON_V3_TC_ESTATUS_DEMANDAJL(Connection con, String campo) throws SQLException {
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    String sql = "SELECT ID FROM  V3_TC_ESTATUS_DEMANDAJL WHERE UPPER(TRIM(DESCRIPCION)) = ?";
    try ( PreparedStatement ps = con.prepareStatement(sql)) {
        ps.setString(1, campo.toUpperCase().trim());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("ID");
            }
            else
            {
            return "Valor Cat No encontrado";
            }
        }
    }
}


public String CON_V3_TC_ESTATUS_EXPEDIENTEJL(Connection con, String campo) throws SQLException {
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    String sql = "SELECT ID FROM 3_TC_ESTATUS_EXPEDIENTEJL WHERE UPPER(TRIM(DESCRIPCION)) = ?";
    try ( PreparedStatement ps = con.prepareStatement(sql)) {
        ps.setString(1, campo.toUpperCase().trim());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("ID");
            }
            else
            {
            return "Valor Cat No encontrado";
            }
        }
    }
}


public String CON_V3_TC_EXISTENCIA_HUELGAJL(Connection con, String campo) throws SQLException {
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    String sql = "SELECT ID FROM V3_TC_EXISTENCIA_HUELGAJL WHERE UPPER(TRIM(DESCRIPCION)) = ?";
    try ( PreparedStatement ps = con.prepareStatement(sql)) {
        ps.setString(1, campo.toUpperCase().trim());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("ID");
            }
            else
            {
            return "Valor Cat No encontrado";
            }
        }
    }
}


public String CON_V3_TC_FASE_CONCLUSION_EJEJL(Connection con, String campo) throws SQLException {
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    String sql = "SELECT ID FROM  V3_TC_FASE_CONCLUSION_EJEJL WHERE UPPER(TRIM(DESCRIPCION)) = ?";
    try ( PreparedStatement ps = con.prepareStatement(sql)) {
        ps.setString(1, campo.toUpperCase().trim());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("ID");
            }
            else
            {
            return "Valor Cat No encontrado";
            }
        }
    }
}


public String CON_V3_TC_FASE_EXPEDIENTEJL(Connection con, String campo) throws SQLException {
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    String sql = "SELECT ID FROM  V3_TC_FASE_EXPEDIENTEJL WHERE UPPER(TRIM(DESCRIPCION)) = ?";
    try ( PreparedStatement ps = con.prepareStatement(sql)) {
        ps.setString(1, campo.toUpperCase().trim());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("ID");
            }
            else
            {
            return "Valor Cat No encontrado";
            }
        }
    }
}


public String CON_V3_TC_FORMA_SOLUCION_HJL(Connection con, String campo) throws SQLException {
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    String sql = "SELECT ID FROM  V3_TC_FORMA_SOLUCION_HJL WHERE UPPER(TRIM(DESCRIPCION)) = ?";
    try ( PreparedStatement ps = con.prepareStatement(sql)) {
        ps.setString(1, campo.toUpperCase().trim());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("ID");
            }
            else
            {
            return "Valor Cat No encontrado";
            }
        }
    }
}


public String CON_V3_TC_FORMA_SOLUCION_PHJL(Connection con, String campo) throws SQLException {
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    String sql = "SELECT ID FROM V3_TC_FORMA_SOLUCION_PHJL WHERE UPPER(TRIM(DESCRIPCION)) = ?";
    try ( PreparedStatement ps = con.prepareStatement(sql)) {
        ps.setString(1, campo.toUpperCase().trim());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("ID");
            }
            else
            {
            return "Valor Cat No encontrado";
            }
        }
    }
}


public String CON_V3_TC_FORMA_SOLUCIONJL(Connection con, String campo) throws SQLException {
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    String sql = "SELECT ID FROM 3_TC_FORMA_SOLUCIONJL WHERE UPPER(TRIM(DESCRIPCION)) = ?";
    try ( PreparedStatement ps = con.prepareStatement(sql)) {
        ps.setString(1, campo.toUpperCase().trim());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("ID");
            }
            else
            {
            return "Valor Cat No encontrado";
            }
        }
    }
}


public String CON_V3_TC_JORNADA_TRABAJADORJL(Connection con, String campo) throws SQLException {
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    String sql = "SELECT ID FROM 3_TC_JORNADA_TRABAJADORJL WHERE UPPER(TRIM(DESCRIPCION)) = ?";
    try ( PreparedStatement ps = con.prepareStatement(sql)) {
        ps.setString(1, campo.toUpperCase().trim());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("ID");
            }
            else
            {
            return "Valor Cat No encontrado";
            }
        }
    }
}


public String CON_V3_TC_JURISDICCIONJL(Connection con, String campo) throws SQLException {
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    String sql = "SELECT ID FROM  V3_TC_JURISDICCIONJL WHERE UPPER(TRIM(DESCRIPCION)) = ?";
    try ( PreparedStatement ps = con.prepareStatement(sql)) {
        ps.setString(1, campo.toUpperCase().trim());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("ID");
            }
            else
            {
            return "Valor Cat No encontrado";
            }
        }
    }
}


public String CON_V3_TC_LICITUD_HUELGAJL(Connection con, String campo) throws SQLException {
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    String sql = "SELECT ID FROM 3_TC_LICITUD_HUELGAJL WHERE UPPER(TRIM(DESCRIPCION)) = ?";
    try ( PreparedStatement ps = con.prepareStatement(sql)) {
        ps.setString(1, campo.toUpperCase().trim());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("ID");
            }
            else
            {
            return "Valor Cat No encontrado";
            }
        }
    }
}


public String CON_V3_TC_MOTIVO_PROMOCIONJL(Connection con, String campo) throws SQLException {
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    String sql = "SELECT ID FROM  V3_TC_MOTIVO_PROMOCIONJL WHERE UPPER(TRIM(DESCRIPCION)) = ?";
    try ( PreparedStatement ps = con.prepareStatement(sql)) {
        ps.setString(1, campo.toUpperCase().trim());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("ID");
            }
            else
            {
            return "Valor Cat No encontrado";
            }
        }
    }
}


public String CON_V3_TC_MOTIVO_SOLICITUDJL(Connection con, String campo) throws SQLException {
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    String sql = "SELECT ID FROM  V3_TC_MOTIVO_SOLICITUDJL WHERE UPPER(TRIM(DESCRIPCION)) = ?";
    try ( PreparedStatement ps = con.prepareStatement(sql)) {
        ps.setString(1, campo.toUpperCase().trim());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("ID");
            }
            else
            {
            return "Valor Cat No encontrado";
            }
        }
    }
}


public String CON_V3_TC_MUNICIPIOJL(Connection con, String campo, int entidadId) throws SQLException {
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    String sql = "SELECT MUNICIPIO_ID FROM V3_TC_MUNICIPIOJL WHERE ENTIDAD_ID = ? AND UPPER(TRIM(DESCRIPCION)) = ?";
    try ( PreparedStatement ps = con.prepareStatement(sql)) {
             ps.setInt(1, entidadId);
          ps.setString(2, campo.toUpperCase().trim());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("MUNICIPIO_ID");
            }
            else
            {
            return "Valor Cat No encontrado";
            }
        }
    }
}


public String CON_V3_TC_NAT_CONFLICTOJL(Connection con, String campo) throws SQLException {
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    String sql = "SELECT ID FROM V3_TC_NAT_CONFLICTOJL WHERE UPPER(TRIM(DESCRIPCION)) = ?";
    try ( PreparedStatement ps = con.prepareStatement(sql)) {
        ps.setString(1, campo.toUpperCase().trim());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("ID");
            }
            else
            {
            return "Valor Cat No encontrado";
            }
        }
    }
}


public String CON_V3_TC_OCUPACION_TRABAJADORJL(Connection con, String campo) throws SQLException {
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    String sql = "SELECT ID FROM  V3_TC_OCUPACION_TRABAJADORJL WHERE UPPER(TRIM(DESCRIPCION)) = ?";
    try ( PreparedStatement ps = con.prepareStatement(sql)) {
        ps.setString(1, campo.toUpperCase().trim());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("ID");
            }
            else
            {
            return "Valor Cat No encontrado";
            }
        }
    }
}


public String CON_V3_TC_ORGAN_OBRERAJL(Connection con, String campo) throws SQLException {
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    String sql = "SELECT ID FROM  V3_TC_ORGAN_OBRERAJL WHERE UPPER(TRIM(DESCRIPCION)) = ?";
    try ( PreparedStatement ps = con.prepareStatement(sql)) {
        ps.setString(1, campo.toUpperCase().trim());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("ID");
            }
            else
            {
            return "Valor Cat No encontrado";
            }
        }
    }
}


public String CON_V3_TC_PROMOVENTEJL(Connection con, String campo) throws SQLException {
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    String sql = "SELECT ID FROM 3_TC_PROMOVENTEJL WHERE UPPER(TRIM(DESCRIPCION)) = ?";
    try ( PreparedStatement ps = con.prepareStatement(sql)) {
        ps.setString(1, campo.toUpperCase().trim());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("ID");
            }
            else
            {
            return "Valor Cat No encontrado";
            }
        }
    }
}


public String CON_V3_TC_RESPUESTA_SIMPLEJL(Connection con, String campo) throws SQLException {
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    String sql = "SELECT ID FROM  V3_TC_RESPUESTA_SIMPLEJL WHERE UPPER(TRIM(DESCRIPCION)) = ?";
    try ( PreparedStatement ps = con.prepareStatement(sql)) {
        ps.setString(1, campo.toUpperCase().trim());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("ID");
            }
            else
            {
            return "Valor Cat No encontrado";
            }
        }
    }
}


public String CON_V3_TC_SECTOR_RAMAJL(Connection con, String campo) throws SQLException {
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    String sql = "SELECT ID_SUBSECTOR FROM  V3_TC_SECTOR_RAMAJL WHERE UPPER(TRIM(DESCRIPCION)) = ?";
    try ( PreparedStatement ps = con.prepareStatement(sql)) {
        ps.setString(1, campo.toUpperCase().trim());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("ID_SUBSECTOR");
            }
            else
            {
            return "Valor Cat No encontrado";
            }
        }
    }
}


public String CON_V3_TC_SENTE_INCIDENTALJL(Connection con, String campo) throws SQLException {
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    String sql = "SELECT ID FROM  V3_TC_SENTE_INCIDENTALJL WHERE UPPER(TRIM(DESCRIPCION)) = ?";
    try ( PreparedStatement ps = con.prepareStatement(sql)) {
        ps.setString(1, campo.toUpperCase().trim());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("ID");
            }
            else
            {
            return "Valor Cat No encontrado";
            }
        }
    }
}


public String CON_V3_TC_SEXO_TRABAJADORJL(Connection con, String campo) throws SQLException {
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    String sql = "SELECT ID FROM  V3_TC_SEXO_TRABAJADORJL WHERE UPPER(TRIM(DESCRIPCION)) = ?";
    try ( PreparedStatement ps = con.prepareStatement(sql)) {
        ps.setString(1, campo.toUpperCase().trim());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("ID");
            }
            else
            {
            return "Valor Cat No encontrado";
            }
        }
    }
}


public String CON_V3_TC_SUBSECTOR_RAMAJL(Connection con, String campo) throws SQLException {
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    String sql = "SELECT ID FROM 3_TC_SUBSECTOR_RAMAJL WHERE UPPER(TRIM(DESCRIPCION)) = ?";
    try ( PreparedStatement ps = con.prepareStatement(sql)) {
        ps.setString(1, campo.toUpperCase().trim());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("ID");
            }
            else
            {
            return "Valor Cat No encontrado";
            }
        }
    }
}


public String CON_V3_TC_TIPO_ASUNTOJL(Connection con, String campo) throws SQLException {
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    String sql = "SELECT ID FROM  V3_TC_TIPO_ASUNTOJL WHERE UPPER(TRIM(DESCRIPCION)) = ?";
    try ( PreparedStatement ps = con.prepareStatement(sql)) {
        ps.setString(1, campo.toUpperCase().trim());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("ID");
            }
            else
            {
            return "Valor Cat No encontrado";
            }
        }
    }
}


public String CON_V3_TC_TIPO_CONTRATOJL(Connection con, String campo) throws SQLException {
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    String sql = "SELECT ID FROM V3_TC_TIPO_CONTRATOJL WHERE UPPER(TRIM(DESCRIPCION)) = ?";
    try ( PreparedStatement ps = con.prepareStatement(sql)) {
        ps.setString(1, campo.toUpperCase().trim());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("ID");
            }
            else
            {
            return "Valor Cat No encontrado";
            }
        }
    }
}


public String CON_V3_TC_TIPO_DEFENSAJL(Connection con, String campo) throws SQLException {
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    String sql = "SELECT ID FROM  V3_TC_TIPO_DEFENSAJL WHERE UPPER(TRIM(DESCRIPCION)) = ?";
    try ( PreparedStatement ps = con.prepareStatement(sql)) {
        ps.setString(1, campo.toUpperCase().trim());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("ID");
            }
            else
            {
            return "Valor Cat No encontrado";
            }
        }
    }
}


public String CON_V3_TC_TIPO_INCIDENTEJL(Connection con, String campo) throws SQLException {
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    String sql = "SELECT ID FROM 3_TC_TIPO_INCIDENTEJL WHERE UPPER(TRIM(DESCRIPCION)) = ?";
    try ( PreparedStatement ps = con.prepareStatement(sql)) {
        ps.setString(1, campo.toUpperCase().trim());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("ID");
            }
            else
            {
            return "Valor Cat No encontrado";
            }
        }
    }
}


public String CON_V3_TC_TIPO_INCOMPETENCIAJL(Connection con, String campo) throws SQLException {
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    String sql = "SELECT ID FROM 3_TC_TIPO_INCOMPETENCIAJL WHERE UPPER(TRIM(DESCRIPCION)) = ?";
    try ( PreparedStatement ps = con.prepareStatement(sql)) {
        ps.setString(1, campo.toUpperCase().trim());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("ID");
            }
            else
            {
            return "Valor Cat No encontrado";
            }
        }
    }
}


public String CON_V3_TC_TIPO_PATRONJL(Connection con, String campo) throws SQLException {
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    String sql = "SELECT ID FROM  V3_TC_TIPO_PATRONJL WHERE UPPER(TRIM(DESCRIPCION)) = ?";
    try ( PreparedStatement ps = con.prepareStatement(sql)) {
        ps.setString(1, campo.toUpperCase().trim());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("ID");
            }
            else
            {
            return "Valor Cat No encontrado";
            }
        }
    }
}


public String CON_V3_TC_TIPO_SENTENCIAJL(Connection con, String campo) throws SQLException {
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    String sql = "SELECT ID FROM 3_TC_TIPO_SENTENCIAJL WHERE UPPER(TRIM(DESCRIPCION)) = ?";
    try ( PreparedStatement ps = con.prepareStatement(sql)) {
        ps.setString(1, campo.toUpperCase().trim());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("ID");
            }
            else
            {
            return "Valor Cat No encontrado";
            }
        }
    }
}


public String CON_V3_TC_TIPO_SINDICATOJL(Connection con, String campo) throws SQLException {
    if (campo == null || campo.trim().isEmpty()) {
        return null;
    }
    String sql = "SELECT ID FROM 3_TC_TIPO_SINDICATOJL WHERE UPPER(TRIM(DESCRIPCION)) = ?";
    try ( PreparedStatement ps = con.prepareStatement(sql)) {
        ps.setString(1, campo.toUpperCase().trim());
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("ID");
            }
            else
            {
            return "Valor Cat No encontrado";
            }
        }
    }
}


 
}
