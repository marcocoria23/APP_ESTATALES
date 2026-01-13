
package LeerQuery.beans;

import Bean_Procedures.Bean;
import java.sql.Connection;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLOutput;

/**
 *
 * @author ALEJANDRO.CASTILLO
 */
public class BeanTR_DEMANDADO extends Bean implements SQLData {
    
        private String SQLTypeName;
        private String ID_DEMANDADO;
        private String ID_ORGANOJ;
        private String ID_TIPO_DEMANDADO;
        private String ID_DEFENSA;
        private String NOMBRE_SINDICATO;
        private String REG_ASOC_SINDICATO;
        private String ID_TIPO_SINDICATO;
        private String PREG_SIND_PERT_ORG_OBR;
        private String ID_ORG_OBR;
        private String CANT_TRAB_INVOL_H;
        private String CANT_TRAB_INVOL_M;
        private String CANT_TRAB_INVOL_NI;
        private String TRAB_INVOL_TOT;
        private String ID_TIPO_PATRON;
        private String RFC;
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
        
        
        public String GetID_DEMANDADO() {
        return ID_DEMANDADO;
    }

        public void SetID_DEMANDADO(String ID_DEMANDADO) {
            this.ID_DEMANDADO = ID_DEMANDADO;
        }
        public String GetID_ORGANOJ() {
            return ID_ORGANOJ;
        }

        public void SetID_ORGANOJ(String ID_ORGANOJ) {
            this.ID_ORGANOJ = ID_ORGANOJ;
        }
        public String GetID_TIPO_DEMANDADO() {
            return ID_TIPO_DEMANDADO;
        }

        public void SetID_TIPO_DEMANDADO(String ID_TIPO_DEMANDADO) {
            this.ID_TIPO_DEMANDADO = ID_TIPO_DEMANDADO;
        }
        public String GetID_DEFENSA() {
            return ID_DEFENSA;
        }

        public void SetID_DEFENSA(String ID_DEFENSA) {
            this.ID_DEFENSA = ID_DEFENSA;
        }
        public String GetNOMBRE_SINDICATO() {
            return NOMBRE_SINDICATO;
        }

        public void SetNOMBRE_SINDICATO(String NOMBRE_SINDICATO) {
            this.NOMBRE_SINDICATO = NOMBRE_SINDICATO;
        }
        public String GetREG_ASOC_SINDICATO() {
            return REG_ASOC_SINDICATO;
        }

        public void SetREG_ASOC_SINDICATO(String REG_ASOC_SINDICATO) {
            this.REG_ASOC_SINDICATO = REG_ASOC_SINDICATO;
        }
        public String GetID_TIPO_SINDICATO() {
            return ID_TIPO_SINDICATO;
        }

        public void SetID_TIPO_SINDICATO(String ID_TIPO_SINDICATO) {
            this.ID_TIPO_SINDICATO = ID_TIPO_SINDICATO;
        }
        public String GetPREG_SIND_PERT_ORG_OBR() {
            return PREG_SIND_PERT_ORG_OBR;
        }

        public void SetPREG_SIND_PERT_ORG_OBR(String PREG_SIND_PERT_ORG_OBR) {
            this.PREG_SIND_PERT_ORG_OBR = PREG_SIND_PERT_ORG_OBR;
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
        public String GetTRAB_INVOL_TOT() {
            return TRAB_INVOL_TOT;
        }

        public void SetTRAB_INVOL_TOT(String TRAB_INVOL_TOT) {
            this.TRAB_INVOL_TOT = TRAB_INVOL_TOT;
        }
        public String GetID_TIPO_PATRON() {
            return ID_TIPO_PATRON;
        }

        public void SetID_TIPO_PATRON(String ID_TIPO_PATRON) {
            this.ID_TIPO_PATRON = ID_TIPO_PATRON;
        }
        public String GetRFC() {
            return RFC;
        }

        public void SetRFC(String RFC) {
            this.RFC = RFC;
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
            GetID_DEMANDADO(),
            GetID_ORGANOJ(),
            GetID_TIPO_DEMANDADO(),
            GetID_DEFENSA(),
            GetNOMBRE_SINDICATO(),
            GetREG_ASOC_SINDICATO(),
            GetID_TIPO_SINDICATO(),
            GetPREG_SIND_PERT_ORG_OBR(),
            GetID_ORG_OBR(),
            GetCANT_TRAB_INVOL_H(),
            GetCANT_TRAB_INVOL_M(),
            GetCANT_TRAB_INVOL_NI(),
            GetTRAB_INVOL_TOT(),
            GetID_TIPO_PATRON(),
            GetRFC(),
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
