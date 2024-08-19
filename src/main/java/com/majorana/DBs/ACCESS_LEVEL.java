package main.Majorana.enum_const;

/**
 *
 *  The Enum ACCESS_LEVEL allows the user to specify who or what is entited to read which files
 */


public enum ACCESS_LEVEL
{

    UNKNOWN(0),
    PUB(1),
    ADMIN(2),

    W3_INNER(3),
    W3_OUTER(4),
    PRIV(5);

    private int accessCode;

     ACCESS_LEVEL(int c){
        accessCode = c;
    }

    public ACCESS_LEVEL getAccessLevelFromCode(int accessLvlCode){///
        for(ACCESS_LEVEL al : values()){
            if (accessLvlCode ==al.accessCode){ return al; }
        }
        return null;
    }



    public int getLevelNum(){
        return accessCode;
    }


}
