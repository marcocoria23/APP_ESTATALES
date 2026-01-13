package Bean_Procedures;

;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public abstract class Bean
{
    
   
   
    public abstract Object[] getLlave();
  
    public abstract Object[] toArray();
    
    public abstract void setAtributos(Object[] obj);       
    
    public abstract void actualizaDesc(Connection conn) throws SQLException ;
  
}