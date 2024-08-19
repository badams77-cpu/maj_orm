package Distiller.ORM;

import Distiller.entities.TableNames;

public class UserEmailJoin {


    private static String FIELDS = ", u_cr.username as created_by_useremail, "+
            "u_up.username as updated_by_useremail ";

    private static String INNER_JOIN = " INNER JOIN ";

    private static String JOIN1 = " u_cr ON en.created_by_userid = u_cr.uid ";
    private static String JOIN2 = "  u_up ON en.updated_by_userid = u_up.uid ";


    private static String JOIN = " INNER JOIN "+ TableNames.USERS + " u_cr ON en.created_by_userid = u_cr.id "+
            " INNER JOIN "+ TableNames.USERS + " enu_up ON .created_by_userid = u_up.id ";


    private String userTable;

    public UserEmailJoin(){
       userTable  = TableNames.USERS;
    }


    public static String getFIELDS() {
        return FIELDS;
    }

    public String getJOIN() {
        return INNER_JOIN + userTable + JOIN1 +
                INNER_JOIN + userTable + JOIN2;
    }
}

