package com.majorana.maj_orm.SCHEME;

import com.majorana.maj_orm.DBs.DatabaseVariant;
import com.majorana.maj_orm.ORM.BaseMajoranaEntity;
import com.majorana.maj_orm.ORM.MajoranaAnnotationRepository;
import com.majorana.maj_orm.ORM.MajoranaRepositoryField;
import com.majorana.maj_orm.ORM_ACCESS.EntityFinder;

import java.io.File;
import java.io.FileWriter;
import java.util.HashSet;
import java.util.Set;

public class WriteSchema {

    public static void main(String argv[]) {
        if (argv.length != 4) {
            System.err.println("Usage: writeschema variant directory base_package");
            return;
        }
        DatabaseVariant variant = DatabaseVariant.valueOf(argv[0]);
        File dir = new File(argv[1]);
        writeSchema(variant, dir,argv[2], Boolean.parseBoolean(argv[3]));
    }

    public static void writeSchema(DatabaseVariant variant, File dir, String bpackage, boolean drop){

        EntityFinder ef = new EntityFinder();
        Set<Class> ec = ef.getEntitiesForPackagePrefix(bpackage);
        System.err.println("Found "+ec.size()+" Domain classes in package "+bpackage);
        if (variant==DatabaseVariant.MYSQL){

            for(Class c : ec){
                System.err.println("Schema for "+c.getCanonicalName());
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
                    Set<String> done = new HashSet();
                    if (drop){
                        fw.write("DROP TABLE IF EXISTS "+name+";\n\n");
                    }
                    fw.write("CREATE TABLE IF NOT EXISTS "+name+" (\n");
                    MajoranaRepositoryField id = mar.getIdField();
                    fw.write(id.getDbColumn()+" int auto_increment primary key\n");
                    for(Object fieldO : mar.getRepoFields()){
                        MajoranaRepositoryField field = (MajoranaRepositoryField)  fieldO;
                        if (field.getDbColumn().equals(id.getDbColumn())){ continue; }
                        if (done.contains(field.getDbColumn())){ continue; }

                        Class fieldType = field.getValueType();
                        if (fieldType.getCanonicalName().equals("java.lang.String")){
                            if (field.getVarcharSize()==0){
                                fw.write(", "+ field.getDbColumn()+" text\n");
                            } else {
                                fw.write(", "+ field.getDbColumn()+" varchar("+field.getVarcharSize() +")\n");
                            }
                        } else {
                            fw.write(", " + field.getDbColumn() + " " + mar.getSqlType(field) + "\n");
                        }
                        done.add(field.getDbColumn());
                    }
                    fw.write(");\n");
                    fw.close();
                    System.out.println("written schema to "+f);
                } catch (Exception e){
                    e.printStackTrace();
                }
            }

        }
    }

}
