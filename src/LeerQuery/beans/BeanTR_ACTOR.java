package LeerQuery.beans;

import Bean_Procedures.Bean;
import java.sql.Connection;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLOutput;

/* @author ALEJANDRO.CASTILLO  */

public class BeanTR_ACTOR extends Bean implements SQLData  {
    
        private String SQLTypeName;      
        private String ID_ACTOR;
        private String ID_ORGANOJ;
        private String ID_TIPO_ACTOR;
        private String ID_DEFENSA;
        private String ID_SEXO;
        private String EDAD;
        private String ID_OCUPACION;
        private String NSS;
        private String CURP;
        private String RFC;
        private String ID_JORNADA;
        private String NOMBRE_SINDICATO;
        private String REGISTRO_ASO_SINDICAL;
        private String ID_TIPO_SINDICATO;
        private String PREG_SIND_PERT_ORG_O;
        private String ID_ORG_OBR;
        private String CANT_TRAB_INVOL_H;
        private String CANT_TRAB_INVOL_M;
        private String CANT_TRAB_INVOL_NI;
        private String TOTAL_TRAB_INVOL;
        private String ID_TIPO_PATRON;
        private String RAZON_SOCIAL;
        private String CALLE;
        private String NUM_EXTERIOR;
        private String NUM_INTERIOR;
        private String COLONIA;
        private String CP;
        private String ID_ENT_MPIO;
        private String LATITUD;
        private String LONGITUD;
        private String COMENTARIOS;
        private String PERIODO;
        private String ID_UNIQUE;
                
     public String GetID_ACTOR() {
        return ID_ACTOR;
    }

    public void SetID_ACTOR(String ID_ACTOR) {
        this.ID_ACTOR = ID_ACTOR;
    }
    
    public String GetID_ORGANOJ() {
        return ID_ORGANOJ;
    }

    public void SetID_ORGANOJ(String ID_ORGANOJ) {
        this.ID_ORGANOJ = ID_ORGANOJ;
    }
    
    public String GetID_TIPO_ACTOR() {
        return ID_TIPO_ACTOR;
    }

    public void SetID_TIPO_ACTOR(String ID_TIPO_ACTOR) {
        this.ID_TIPO_ACTOR = ID_TIPO_ACTOR;
    }
    
    public String GetID_DEFENSA() {
        return ID_DEFENSA;
    }

    public void SetID_DEFENSA(String ID_DEFENSA) {
        this.ID_DEFENSA = ID_DEFENSA;
    }
    
    public String GetID_SEXO() {
        return ID_SEXO;
    }

    public void SetID_SEXO(String ID_SEXO) {
        this.ID_SEXO = ID_SEXO;
    } 
    
    public String GetEDAD() {
        return EDAD;
    }

    public void SetEDAD(String EDAD) {
        this.EDAD = EDAD;
    }
    
    public String GetID_OCUPACION() {
        return ID_OCUPACION;
    }

    public void SetID_OCUPACION(String ID_OCUPACION) {
        this.ID_OCUPACION = ID_OCUPACION;
    }
    
    public String GetNSS() {
        return NSS;
    }

    public void SetNSS(String NSS) {
        this.NSS = NSS;
    }
    
    public String GetCURP() {
        return CURP;
    }

    public void SetCURP(String CURP) {
        this.CURP = CURP;
    }
    
    public String GetRFC() {
        return RFC;
    }

    public void SetRFC(String RFC) {
        this.RFC = RFC;
    }
    
    public String GetID_JORNADA() {
        return ID_JORNADA;
    }

    public void SetID_JORNADA(String ID_JORNADA) {
        this.ID_JORNADA = ID_JORNADA;
    } 
    
    public String GetNOMBRE_SINDICATO() {
        return NOMBRE_SINDICATO;
    }

    public void SetNOMBRE_SINDICATO(String NOMBRE_SINDICATO) {
        this.NOMBRE_SINDICATO = NOMBRE_SINDICATO;
    }
    
    public String GetREGISTRO_ASO_SINDICAL() {
        return REGISTRO_ASO_SINDICAL;
    }

    public void SetREGISTRO_ASO_SINDICAL(String REGISTRO_ASO_SINDICAL) {
        this.REGISTRO_ASO_SINDICAL = REGISTRO_ASO_SINDICAL;
    }
    
    public String GetID_TIPO_SINDICATO() {
        return ID_TIPO_SINDICATO;
    }

    public void SetID_TIPO_SINDICATO(String ID_TIPO_SINDICATO) {
        this.ID_TIPO_SINDICATO = ID_TIPO_SINDICATO;
    }
    
    public String GetPREG_SIND_PERT_ORG_O() {
        return PREG_SIND_PERT_ORG_O;
    }

    public void SetPREG_SIND_PERT_ORG_O(String PREG_SIND_PERT_ORG_O) {
        this.PREG_SIND_PERT_ORG_O = PREG_SIND_PERT_ORG_O;
    }
    
    public String GetID_ORG_OBR() {
        return ID_ORG_OBR;
    }

    public void SetID_ORG_OBR(String ID_ORG_OBR) {
        this.ID_ORG_OBR = ID_ORG_OBR;
    }
    
    public String GetCANT_TRAB_INVOL_H() {
        return CANT_TRAB_INVOL_H;
    }

    public void SetCANT_TRAB_INVOL_H(String CANT_TRAB_INVOL_H) {
        this.CANT_TRAB_INVOL_H = CANT_TRAB_INVOL_H;
    }
    
    public String GetCANT_TRAB_INVOL_M() {
        return CANT_TRAB_INVOL_M;
    }

    public void SetCANT_TRAB_INVOL_M(String CANT_TRAB_INVOL_M) {
        this.CANT_TRAB_INVOL_M = CANT_TRAB_INVOL_M;
    }
    
    public String GetCANT_TRAB_INVOL_NI() {
        return CANT_TRAB_INVOL_NI;
    }

    public void SetCANT_TRAB_INVOL_NI(String CANT_TRAB_INVOL_NI) {
        this.CANT_TRAB_INVOL_NI = CANT_TRAB_INVOL_NI;
    } 
    
    public String GetTOTAL_TRAB_INVOL() {
        return TOTAL_TRAB_INVOL;
    }

    public void SetTOTAL_TRAB_INVOL(String TOTAL_TRAB_INVOL) {
        this.TOTAL_TRAB_INVOL = TOTAL_TRAB_INVOL;
    } 
    
    public String GetID_TIPO_PATRON() {
        return ID_TIPO_PATRON;
    }

    public void SetID_TIPO_PATRON(String ID_TIPO_PATRON) {
        this.ID_TIPO_PATRON = ID_TIPO_PATRON;
    }
    
    public String GetRAZON_SOCIAL() {
        return RAZON_SOCIAL;
    }

    public void SetRAZON_SOCIAL(String RAZON_SOCIAL) {
        this.RAZON_SOCIAL = RAZON_SOCIAL;
    }
    
    public String GetCALLE() {
        return CALLE;
    }

    public void SetCALLE(String CALLE) {
        this.CALLE = CALLE;
    }
    
    public String GetNUM_EXTERIOR() {
        return NUM_EXTERIOR;
    }

    public void SetNUM_EXTERIOR(String NUM_EXTERIOR) {
        this.NUM_EXTERIOR = NUM_EXTERIOR;
    }
    
    public String GetNUM_INTERIOR() {
        return NUM_INTERIOR;
    }

    public void SetNUM_INTERIOR(String NUM_INTERIOR) {
        this.NUM_INTERIOR = NUM_INTERIOR;
    }
   
    public String GetCOLONIA() {
        return COLONIA;
    }

    public void SetCOLONIA(String COLONIA) {
        this.COLONIA = COLONIA;
    }
    
    public String GetCP() {
        return CP;
    }

    public void SetCP(String CP) {
        this.CP = CP;
    }
    
    public String GetID_ENT_MPIO() {
        return ID_ENT_MPIO;
    }

    public void SetID_ENT_MPIO(String ID_ENT_MPIO) {
        this.ID_ENT_MPIO = ID_ENT_MPIO;
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
    
    public String GetCOMENTARIOS() {
        return COMENTARIOS;
    }

    public void SetCOMENTARIOS(String COMENTARIOS) {
        this.COMENTARIOS = COMENTARIOS;
    }
    
    public String GetPERIODO() {
        return PERIODO;
    }

    public void SetPERIODO(String PERIODO) {
        this.PERIODO = PERIODO;
    }
    
    public String GetID_UNIQUE() {
        return ID_UNIQUE;
    }

    public void SetID_UNIQUE(String ID_UNIQUE) {
        this.ID_UNIQUE = ID_UNIQUE;
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
            GetID_ACTOR(),
            GetID_ORGANOJ(),
            GetID_TIPO_ACTOR(),
            GetID_DEFENSA(),
            GetID_SEXO(),
            GetEDAD(),
            GetID_OCUPACION(),
            GetNSS(),
            GetCURP(),
            GetRFC(),
            GetID_JORNADA(),
            GetNOMBRE_SINDICATO(),
            GetREGISTRO_ASO_SINDICAL(),
            GetID_TIPO_SINDICATO(),
            GetPREG_SIND_PERT_ORG_O(),
            GetID_ORG_OBR(),
            GetCANT_TRAB_INVOL_H(),
            GetCANT_TRAB_INVOL_M(),
            GetCANT_TRAB_INVOL_NI(),
            GetTOTAL_TRAB_INVOL(),
            GetID_TIPO_PATRON(),
            GetRAZON_SOCIAL(),
            GetCALLE(),
            GetNUM_EXTERIOR(),
            GetNUM_INTERIOR(),
            GetCOLONIA(),
            GetCP(),
            GetID_ENT_MPIO(),
            GetLATITUD(),
            GetLONGITUD(),
            GetCOMENTARIOS(),
            GetPERIODO(),
            GetID_UNIQUE()
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
