package com.majorana.maj_orm.SCHEME;

import com.majorana.maj_orm.DBs.DatabaseVariant;
import com.majorana.maj_orm.ORM.BaseMajoranaEntity;
import com.majorana.maj_orm.ORM.MajoranaAnnotationRepository;
import com.majorana.maj_orm.ORM.MajoranaRepositoryField;
import com.majorana.maj_orm.ORM_ACCESS.EntityFinder;

import java.io.File;
import java.io.FileWriter;
import java.util.Set;

public class WriteSchema {

    public static void main(String argv[]) {
        if (argv.length != 3) {
            System.err.println("Usage: writeschema variant directory base_package");
            return;
        }

        DatabaseVariant variant = DatabaseVariant.valueOf(argv[0]);
        File dir = new File(argv[1]);
        writeSchema(variant, dir,argv[2]);
    }

    public static void writeSchema(DatabaseVariant variant, File dir, String bpackage){

        EntityFinder ef = new EntityFinder();
        Set<Class> ec = ef.getEntitiesForPackagePrefix(bpackage);
        if (variant==DatabaseVariant.MYSQL){

            for(Class c : ec){
                try {
                    String name = c.getCanonicalName();
                    Object entity = c.newInstance();
                    MajoranaAnnotationRepository mar
                            = new MajoranaAnnotationRepository(null, null, c);
                    if (entity instanceof BaseMajoranaEntity) {
                        name = ((BaseMajoranaEntity) entity).getTableName();
                    }
                    File f = new File(dir, name + ".sql");
                    FileWriter fw = new FileWriter(f);
                    fw.write("CREATE TABLE "+name+" IF NOT EXISTS (\n");
                    MajoranaRepositoryField id = mar.getIdField();
                    fw.write(id.getDbColumn()+" int auto_increament primary key");
                    for(Object fieldO : mar.getRepoFields()){
                        MajoranaRepositoryField field = (MajoranaRepositoryField)  fieldO;
                        if (field.getDbColumn().equals(id.getDbColumn())){ continue; }
                        Class fieldType = field.getValueType();
                        if (fieldType.getCanonicalName().equals("java.lang.String")){
                            if (field.getVarcharSize()==0){
                                fw.write(", "+ field.getDbColumn()+" text\n");
                            } else {
                                fw.write(", "+ field.getDbColumn()+" varchar("+field.getVarcharSize() +")\n");
                            }
                        }
                        fw.write(", "+field.getDbColumn()+" "+mar.getSqlType(field)+"\n");
                    }
                    fw.write(");\n");
                    fw.close();
                } catch (Exception e){}
            }

        }
    }

}
