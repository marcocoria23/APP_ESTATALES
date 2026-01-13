
package LeerQuery.beans;

import Bean_Procedures.Bean;
import java.sql.Connection;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLOutput;

/*
 * @author ALEJANDRO.CASTILLO
 */
public class BeanTR_GENERAL extends Bean implements SQLData  {
    
        private String SQLTypeName;
        private String ID_GENERAL;
        private String ID_ORGANOJ;
        private String JUEZ_INDIVIDUA_H;
        private String JUEZ_INDIVIDUAL_M;
        private String JUEZ_COLECTIVO_H;
        private String JUEZ_COLECTIVO_M;
        private String JUEZ_MIXTO_H;
        private String JUEZ_MIXTOS_M;
        private String SUBTOTAL_JUEZ_H;
        private String SUBTOTAL_JUEZ_M;
        private String TOTAL_JUECES;
        private String TOTAL_ORDINARIO;
        private String TOTAL_INDIVIDUAL;
        private String TOTAL_COLECTIVO;
        private String TOTAL_HUELGA;
        private String TOTAL_COL_NAT_ECON;
        private String TOTAL_PARAPROCESAL;
        private String TOTAL_TERCERIAS;
        private String TOTAL_PREF_CRED;
        private String TOTAL_EJECUCION;
        private String PERIODO;
        
        
        public String GetID_GENERAL() {
        return ID_GENERAL;
    }

    public void SetID_GENERAL(String ID_GENERAL) {
        this.ID_GENERAL = ID_GENERAL;
    }
    
    public String GetID_ORGANOJ() {
        return ID_ORGANOJ;
    }

    public void SetID_ORGANOJ(String ID_ORGANOJ) {
        this.ID_ORGANOJ = ID_ORGANOJ;
    }
    
    public String GetJUEZ_INDIVIDUA_H() {
        return JUEZ_INDIVIDUA_H;
    }

    public void SetJUEZ_INDIVIDUA_H(String JUEZ_INDIVIDUA_H) {
        this.JUEZ_INDIVIDUA_H = JUEZ_INDIVIDUA_H;
    }
    
    public String GetJUEZ_INDIVIDUAL_M() {
        return JUEZ_INDIVIDUAL_M;
    }

    public void SetJUEZ_INDIVIDUAL_M(String JUEZ_INDIVIDUAL_M) {
        this.JUEZ_INDIVIDUAL_M = JUEZ_INDIVIDUAL_M;
    }
    
    public String GetJUEZ_COLECTIVO_H() {
        return JUEZ_COLECTIVO_H;
    }

    public void SetJUEZ_COLECTIVO_H(String JUEZ_COLECTIVO_H) {
        this.JUEZ_COLECTIVO_H = JUEZ_COLECTIVO_H;
    }
    
   public String GetJUEZ_COLECTIVO_M() {
        return JUEZ_COLECTIVO_M;
    }

    public void SetJUEZ_COLECTIVO_M(String JUEZ_COLECTIVO_M) {
        this.JUEZ_COLECTIVO_M = JUEZ_COLECTIVO_M;
    } 
   
    public String GetJUEZ_MIXTO_H() {
        return JUEZ_MIXTO_H;
    }

    public void SetJUEZ_MIXTO_H(String JUEZ_MIXTO_H) {
        this.JUEZ_MIXTO_H = JUEZ_MIXTO_H;
    }
    
    public String GetJUEZ_MIXTOS_M() {
        return JUEZ_MIXTOS_M;
    }

    public void SetJUEZ_MIXTOS_M(String JUEZ_MIXTOS_M) {
        this.JUEZ_MIXTOS_M = JUEZ_MIXTOS_M;
    }
    
  public String GetSUBTOTAL_JUEZ_H() {
        return SUBTOTAL_JUEZ_H;
    }

    public void SetSUBTOTAL_JUEZ_H(String SUBTOTAL_JUEZ_H) {
        this.SUBTOTAL_JUEZ_H = SUBTOTAL_JUEZ_H;
    }
     
    public String GetSUBTOTAL_JUEZ_M() {
        return SUBTOTAL_JUEZ_M;
    }

    public void SetSUBTOTAL_JUEZ_M(String SUBTOTAL_JUEZ_M) {
        this.SUBTOTAL_JUEZ_M = SUBTOTAL_JUEZ_M;
    }
    
    public String GetTOTAL_JUECES() {
        return TOTAL_JUECES;
    }

    public void SetTOTAL_JUECES(String TOTAL_JUECES) {
        this.TOTAL_JUECES = TOTAL_JUECES;
    }

    public String GetTOTAL_ORDINARIO() {
        return TOTAL_ORDINARIO;
    }

    public void SetTOTAL_ORDINARIO(String TOTAL_ORDINARIO) {
        this.TOTAL_ORDINARIO = TOTAL_ORDINARIO;
    }

    public String GetTOTAL_INDIVIDUAL() {
        return TOTAL_INDIVIDUAL;
    }

    public void SetTOTAL_INDIVIDUAL(String TOTAL_INDIVIDUAL) {
        this.TOTAL_INDIVIDUAL = TOTAL_INDIVIDUAL;
    }
    
    public String GetTOTAL_COLECTIVO() {
        return TOTAL_COLECTIVO;
    }

    public void SetTOTAL_COLECTIVO(String TOTAL_COLECTIVO) {
        this.TOTAL_COLECTIVO = TOTAL_COLECTIVO;
    }
    
    public String GetTOTAL_HUELGA() {
        return TOTAL_HUELGA;
    }

    public void SetTOTAL_HUELGA(String TOTAL_HUELGA) {
        this.TOTAL_HUELGA = TOTAL_HUELGA;
    }
    
   public String GetTOTAL_COL_NAT_ECON() {
        return TOTAL_COL_NAT_ECON;
    }

    public void SetTOTAL_COL_NAT_ECON(String TOTAL_COL_NAT_ECON) {
        this.TOTAL_COL_NAT_ECON = TOTAL_COL_NAT_ECON;
    }
    
   public String GetTOTAL_PARAPROCESAL() {
        return TOTAL_PARAPROCESAL;
    }

    public void SetTOTAL_PARAPROCESAL(String TOTAL_PARAPROCESAL) {
        this.TOTAL_PARAPROCESAL = TOTAL_PARAPROCESAL;
    }
    
    public String GetTOTAL_TERCERIAS() {
        return TOTAL_TERCERIAS;
    }

    public void SetTOTAL_TERCERIAS(String TOTAL_TERCERIAS) {
        this.TOTAL_TERCERIAS = TOTAL_TERCERIAS;
    }
    
    public String GetTOTAL_PREF_CRED() {
        return TOTAL_PREF_CRED;
    }

    public void SetTOTAL_PREF_CRED(String TOTAL_PREF_CRED) {
        this.TOTAL_PREF_CRED = TOTAL_PREF_CRED;
    }
    
    public String GetTOTAL_EJECUCION() {
        return TOTAL_EJECUCION;
    }

    public void SetTOTAL_EJECUCION(String TOTAL_EJECUCION) {
        this.TOTAL_EJECUCION = TOTAL_EJECUCION;
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
        GetID_GENERAL(),
        GetID_ORGANOJ(),
        GetJUEZ_INDIVIDUA_H(),
        GetJUEZ_INDIVIDUAL_M(),
        GetJUEZ_COLECTIVO_H(),
        GetJUEZ_COLECTIVO_M(),
        GetJUEZ_MIXTO_H(),
        GetJUEZ_MIXTOS_M(),
        GetSUBTOTAL_JUEZ_H(),
        GetSUBTOTAL_JUEZ_M(),
        GetTOTAL_JUECES(),
        GetTOTAL_ORDINARIO(),
        GetTOTAL_INDIVIDUAL(),
        GetTOTAL_COLECTIVO(),
        GetTOTAL_HUELGA(),
        GetTOTAL_COL_NAT_ECON(),
        GetTOTAL_PARAPROCESAL(),
        GetTOTAL_TERCERIAS(),
        GetTOTAL_PREF_CRED(),
        GetTOTAL_EJECUCION(),
        GetPERIODO()

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
