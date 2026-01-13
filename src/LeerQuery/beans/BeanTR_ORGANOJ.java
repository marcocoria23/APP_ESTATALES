package LeerQuery.beans;

import Bean_Procedures.Bean;
import java.sql.Connection;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLOutput;

/**
 * @author ALEJANDRO.CASTILLO
 */
public class BeanTR_ORGANOJ extends Bean implements SQLData {

    private String SQLTypeName;      
    private String ID_ORGANOJ;
    private String NOMBRE_ORGANOJ;
    private String SEDE_ORGANOJ;
    private String ID_ENT_MPIO;
    private String COLONIA;
    private String LATITUD;
    private String LONGITUD;
    private String ID_CIRCUNSCRIPCION;
    private String ID_JURISDICCION;
    private String HR_ATENCION;
    private String PERIODO;
    private String ESTATUS;
    
    public String GetID_ORGANOJ() {
        return ID_ORGANOJ;
    }

    public void SetID_ORGANOJ(String ID_ORGANOJ) {
        this.ID_ORGANOJ = ID_ORGANOJ;
    }
    
     public String GetESTATUS() {
        return ESTATUS;
    }

    public void SetESTATUS(String ESTATUS) {
        this.ESTATUS = ESTATUS;
    }
    
    public String GetNOMBRE_ORGANOJ() {
        return NOMBRE_ORGANOJ;
    }

    public void SetNOMBRE_ORGANOJ(String NOMBRE_ORGANOJ) {
        this.NOMBRE_ORGANOJ = NOMBRE_ORGANOJ;
    }
    
    public String GetSEDE_ORGANOJ() {
        return SEDE_ORGANOJ;
    }

    public void SetSEDE_ORGANOJ(String SEDE_ORGANOJ) {
        this.SEDE_ORGANOJ = SEDE_ORGANOJ;
    }

     public String GetID_ENT_MPIO() {
        return ID_ENT_MPIO;
    }

    public void SetID_ENT_MPIO(String ID_ENT_MPIO) {
        this.ID_ENT_MPIO = ID_ENT_MPIO;
    }

    public String GetCOLONIA() {
        return COLONIA;
    }

    public void SetCOLONIA(String COLONIA) {
        this.COLONIA = COLONIA;
    }

    public String GetLATITUD() {
        return LATITUD;
    }

    public void SetLATITUD(String LATITUD) {
        this.LATITUD = LATITUD;
    }
    
    public String GetLONGITUD() {
        return LONGITUD;
    }

    public void SetLONGITUD(String LONGITUD) {
        this.LONGITUD = LONGITUD;
    }
    
    public String GetID_CIRCUNSCRIPCION() {
        return ID_CIRCUNSCRIPCION;
    }

    public void SetID_CIRCUNSCRIPCION(String ID_CIRCUNSCRIPCION) {
        this.ID_CIRCUNSCRIPCION = ID_CIRCUNSCRIPCION;
    }
    
    public String GetID_JURISDICCION() {
        return ID_JURISDICCION;
    }

    public void SetID_JURISDICCION(String ID_JURISDICCION) {
        this.ID_JURISDICCION = ID_JURISDICCION;
    }
    
    public String GetHR_ATENCION() {
        return HR_ATENCION;
    }

    public void SetHR_ATENCION(String HR_ATENCION) {
        this.HR_ATENCION = HR_ATENCION;
    }
    
    public String GetPERIODO() {
        return PERIODO;
    }

    public void SetPERIODO(String PERIODO) {
        this.PERIODO = PERIODO;
    }
    
    @Override
    public Object[] getLlave() {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }
    
    @Override
    public void setAtributos(Object[] obj) {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public void actualizaDesc(Connection conn) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

   
    @Override
    public void readSQL(SQLInput stream, String typeName) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public void writeSQL(SQLOutput stream) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }
    
    @Override
    public String getSQLTypeName() throws SQLException {
       return SQLTypeName;
    }
    
    
      @Override
    public Object[] toArray() {
       Object[] array = {
            GetID_ORGANOJ(),
            GetNOMBRE_ORGANOJ(),
            GetSEDE_ORGANOJ(),
            GetID_ENT_MPIO(),
            GetCOLONIA(),
            GetLATITUD(),
            GetLONGITUD(),
            GetID_CIRCUNSCRIPCION(),
            GetID_JURISDICCION(),
            GetHR_ATENCION(),
            GetPERIODO(),
            GetESTATUS()
        };
        return array;
    }
    
     private String ots(Object obj) {
        String val = null;
        if (obj != null) {
            val = obj.toString();
        }
        return val;
    }

    private Integer oti(Object obj) {
        Integer num = null;
        if (obj != null) {
            try {
                num = (Integer) obj;
            } catch (Exception ex) {
                System.out.println("Error en casteo de numero [" + obj + "]");
            }
        }
        return num;
    }
    
}
