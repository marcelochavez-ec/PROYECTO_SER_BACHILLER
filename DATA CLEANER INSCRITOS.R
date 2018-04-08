#============================================================================================================
# SCRIPTING PARA DEPURACIÓN Y ESTRUCTURACIÓN DE LA BDD DE INSCRITOS - SER BACHILLER
# FUENTE: Vistas materializadas del Sistema SNNA
# Fecha: 02/marzo/2018
# Developer: Ing. Marcelo Chávez
# Repositorio GITHUB para versionamiento: https://github.com/marcelochavez-ec/PROYECTO_SER_BACHILLER
#============================================================================================================
# BDD INSCRITOS:
# PAQUETES:
library(RPostgreSQL)
library(dplyr)
library(plyr)
library(reshape2)
library(lubridate)
library(xlsx)
#============================================================================================================
# LECTURA DE LAS ZONAS DE PLANIFICACIÓN SENPLADES Y SENESCYT:
zonas<-read.xlsx2("/home/marcelo/Documents/PROYECTO_BI_SENESCYT_2/BDD_FUENTES/VISTAS/DPA_ZONAS.xlsx",
                  sheetName = "ZONAS",as.data.frame = T,header = T)
zonas<-data.frame(lapply(zonas, as.character), stringsAsFactors=F)
#============================================================================================================
senescyt_bi<-dbConnect("PostgreSQL",dbname="senescyt_bi",
                  host="localhost",port=5432,user="postgres",
                  password="postgres")
#============================================================================================================
# Función para calcular la edad del estudiante:
calculo_edad<-function(from, to) 
{
  to=Sys.Date()
  from_lt = as.POSIXlt(from)
  to_lt = as.POSIXlt(to)
  age = to_lt$year - from_lt$year
  ifelse(to_lt$mon < from_lt$mon |
           (to_lt$mon == from_lt$mon & to_lt$mday < from_lt$mday),
         age - 1, age)
}
#============================================================================================================
# LECTURA DE LAS BDD:
# NOTA METODOLÓGICA: Dependerá del repositorio donde se este programando para cambiar el path de 
# la lectura de las BDD

load("/home/marcelo/Documents/PROYECTO_BI_SENESCYT_2/BDD_FUENTES/VISTAS/inscritos_1.RData")
inscritos_1<-as.data.frame(lapply(inscritos_1,function(x) if(is.character(x))
  iconv(x,"UTF-8","UTF-8") else x),stringsAsFactors=F)

inscritos_1<-as.data.frame(
  lapply(inscritos_1, function(x)
    if(is.character(x))
    {
      trimws(x)
      gsub("^$","SIN REGISTRO",x)
      ifelse(is.na(x),"SIN REGISTRO",x)
      toupper(x)
    }
    else
    {
      x
    }),
  stringsAsFactors=F)

load("/home/marcelo/Documents/PROYECTO_BI_SENESCYT_2/BDD_FUENTES/VISTAS/inscritos_2.RData")
inscritos_2<-as.data.frame(lapply(inscritos_2,function(x) if(is.character(x))
  iconv(x,"UTF-8","UTF-8") else x),stringsAsFactors=F)

inscritos_2<-as.data.frame(
  lapply(inscritos_2, function(x)
    if(is.character(x))
    {
      trimws(x)
      gsub("^$","SIN REGISTRO",x)
      ifelse(is.na(x),"SIN REGISTRO",x)
      toupper(x)
    }
    else
    {
      x
    }),
  stringsAsFactors=F)


load("/home/marcelo/Documents/PROYECTO_BI_SENESCYT_2/BDD_FUENTES/VISTAS/inscritos_3.RData")
inscritos_3<-as.data.frame(lapply(inscritos_3,function(x) if(is.character(x))
  iconv(x,"UTF-8","UTF-8") else x),stringsAsFactors=F)

inscritos_3<-as.data.frame(
  lapply(inscritos_3, function(x)
    if(is.character(x))
    {
      trimws(x)
      gsub("^$","SIN REGISTRO",x)
      ifelse(is.na(x),"SIN REGISTRO",x)
      toupper(x)
    }
    else
    {
      x
    }),
  stringsAsFactors=F)

load("/home/marcelo/Documents/PROYECTO_BI_SENESCYT_2/BDD_FUENTES/VISTAS/inscritos_p15.RData")
inscritos_p15<-as.data.frame(lapply(inscritos_p15,function(x) if(is.character(x))
  iconv(x,"UTF-8","UTF-8") else x),stringsAsFactors=F)

inscritos_p15<-as.data.frame(
  lapply(inscritos_p15, function(x)
    if(is.character(x))
    {
      trimws(x)
      gsub("^$","SIN REGISTRO",x)
      ifelse(is.na(x),"SIN REGISTRO",x)
      toupper(x)
    }
    else
    {
      x
    }),
  stringsAsFactors=F)

#============================================================================================================
# SELECCIÓN DE VARIABLES:

ins_1<-inscritos_1 %>% 
  mutate(#Variables que se crean para identificar al colegio de procedencia:
    edad=calculo_edad(fecha_nacimiento),#32
    #------VARIABLES DEL PER13 AL PER14:
    escolaridad="SIN REGISTRO",#33
    ued_id="SIN REGISTRO",#34
    ued_amie="SIN REGISTRO",#35
    prd_jornada="SIN REGISTRO",#36
    codigo_parroquia_ue="SIN REGISTRO",#37
    estado_civil="SIN REGISTRO",#38
    sustentantes=ifelse(recinto_asignado!="NA",1,0)) %>% 
  select(
    #Variables del estudiante:
    per_id,#1
    prq_id_reside,#2
    prq_id_nace,#3
    usu_id,#4
    usu_cc,#5
    usu_tipo_doc,#6
    dpa_genero,#7
    usu_nombres,#8
    usu_apellidos,#9
    fecha_nacimiento,#10
    etnia,#11
    agrupacion_etnica,#12
    rinde_enes,#13
    pais_reside,#14
    pais_nace,#15
    es_discapacitado,#16
    dpa_tipo_discapacidad,#17
    dpa_grado_discapacidad,#18
    dpa_carnet_conadis,#19
    dpa_residencia,#20
    dpa_telefono,#21
    dpa_celular,#22
    bdh,#23
    ced_beneficiario_bdh,#24
    usu_email,#25
    #Variables del colegio de procedencia:
    nombre_ued,#26
    tipo_ued,#27
    #Variables de las calificaciones por dominios:
    nota_verbal,#28
    nota_logica,#29
    nota_abstracta,#30
    #nota_enes,
    nota_postula,
    edad,
    escolaridad,
    ued_id,
    ued_amie,
    prd_jornada,
    codigo_parroquia_ue,
    estado_civil,
    sustentantes) %>% #31
  plyr::rename(c("usu_tipo_doc"="tipo_documento",
                 "dpa_genero"="sexo",
                 "usu_nombres"="nombres",
                 "usu_apellidos"="apellidos",
                 "etnia"="autoidentificacion",
                 "agrupacion_etnica"="pueblos_nacionalidades",
                 "nombre_ued"="ued_nombre",
                 "tipo_ued"="ued_tipo",
                 "dpa_tipo_discapacidad"="tipo_discapacidad",
                 "dpa_grado_discapacidad"="grado_discapacidad",
                 "dpa_carnet_conadis"="nro_carnet_conadis",
                 "dpa_residencia"="domicilio",
                 "dpa_telefono"="telefono",
                 "dpa_celular"="movil",
                 "usu_email"="email",
                 "rinde_enes"="rinde_examen"))

ins_2<-inscritos_2 %>%
  mutate(#Variables que se crean para identificar al colegio de procedencia:
    edad=calculo_edad(fecha_nacimiento),#32
    #------VARIABLES DEL PER13 AL PER14:
    escolaridad="SIN REGISTRO",#33
    ued_id="SIN REGISTRO",#34
    ued_amie="SIN REGISTRO",#35
    prd_jornada="SIN REGISTRO",#36
    codigo_parroquia_ue="SIN REGISTRO",#37
    estado_civil="SIN REGISTRO",#38
    sustentantes=ifelse(recinto_asignado!="NA",1,0)) %>% 
  select(
    #Variables del estudiante:
    per_id,#1
    prq_id_reside,#2
    prq_id_nace,#3
    usu_id,#4
    usu_cc,#5
    usu_tipo_doc,#6
    dpa_genero,#7
    usu_nombres,#8
    usu_apellidos,#9
    fecha_nacimiento,#10
    etnia,#11
    agrupacion_etnica,#12
    rinde_enes,#13
    pais_reside,#14
    pais_nace,#15
    es_discapacitado,#16
    dpa_tipo_discapacidad,#17
    dpa_grado_discapacidad,#18
    dpa_carnet_conadis,#19
    dpa_residencia,#20
    dpa_telefono,#21
    dpa_celular,#22
    bdh,#23
    ced_beneficiario_bdh,#24
    usu_email,#25
    #Variables del colegio de procedencia:
    nombre_ued,#26
    tipo_ued,#27
    #Variables de las calificaciones por dominios:
    nota_verbal,#28
    nota_logica,#29
    nota_abstracta,#30
    #nota_enes,
    nota_postula,
    edad,
    escolaridad,
    ued_id,
    ued_amie,
    prd_jornada,
    codigo_parroquia_ue,
    estado_civil,
    sustentantes) %>% #31
  plyr::rename(c("usu_tipo_doc"="tipo_documento",
                 "dpa_genero"="sexo",
                 "usu_nombres"="nombres",
                 "usu_apellidos"="apellidos",
                 "etnia"="autoidentificacion",
                 "agrupacion_etnica"="pueblos_nacionalidades",
                 "nombre_ued"="ued_nombre",
                 "tipo_ued"="ued_tipo",
                 "dpa_tipo_discapacidad"="tipo_discapacidad",
                 "dpa_grado_discapacidad"="grado_discapacidad",
                 "dpa_carnet_conadis"="nro_carnet_conadis",
                 "dpa_residencia"="domicilio",
                 "dpa_telefono"="telefono",
                 "dpa_celular"="movil",
                 "usu_email"="email",
                 "rinde_enes"="rinde_examen"))

ins_3<-inscritos_3 %>% 
    mutate(#Variables que se crean para identificar al colegio de procedencia:
    #------VARIABLES DEL PER2 AL PER12:
    nota_verbal=9999,
    nota_logica=9999,
    nota_abstracta=9999,
    prq_id_nace="",
    edad=calculo_edad(fecha_nacimiento),
    es_discapacitado="SIN REGISTRO",
    sustentantes=case_when(
                 ins_estado=="T" & per_id==13 ~ 1,
                 ins_sede_rec_asignado!="NA" & per_id==14 ~ 1)) %>% 
  mutate(domicilio=paste(ins_calle_principal,
                         ins_calle_secundaria,
                         ins_barrio_sector,
                         ins_referencia,
                         ins_num_casa,
                         sep="|")) %>%
  select(#Variables del estudiante:
    per_id,#1
    codigo_parroquia_resid,#2
    usu_id,#3
    cedula,#4
    tipo_documento,#5
    sexo,#6
    nombres,#7
    apellidos,#8
    fecha_nacimiento,#9
    ins_autoidentificacion,#10
    ins_nacionalidad,#11
    cae_rinde_examen,#12
    pais_reside,#13
    pais_nacimiento,#14
    ins_tipo_discapacidad,#15
    ins_grado_discapacidad,#16
    ins_carnet_conadis,#17
    domicilio,#18 
    telefono2,#19
    celular,#20
    email,#21
    estado_civil,#22
    ins_cc_beneficiario,#23
    ins_hogar_bdh,#24
    ins_poblacion,#25
    #Variables del colegio de procedencia:
    ued_id,#26
    ued_nombre,#27
    ued_tipo,#28
    ued_amie,#29
    codigo_parroquia_ue,#30
    prd_jornada,#31
    #Variables de las calificaciones por dominios:
    cae_nota_enes,#32
    #Variables de control:
    nota_verbal,#33
    nota_logica,#34
    nota_abstracta,#35
    prq_id_nace,#36
    edad,#37
    es_discapacitado,
    sustentantes) %>% #38
  plyr::rename(c("ins_autoidentificacion"="autoidentificacion",
                 "ins_nacionalidad"="pueblos_nacionalidades",
                 "ins_tipo_discapacidad"="tipo_discapacidad",
                 "ins_grado_discapacidad"="grado_discapacidad",
                 "ins_carnet_conadis"="nro_carnet_conadis",
                 "telefono2"="telefono",
                 "celular"="movil",
                 "ins_poblacion"="escolaridad",
                 "codigo_parroquia_resid"="prq_id_reside",
                 "cedula"="usu_cc",
                 "cae_nota_enes"="nota_postula",
                 "ins_hogar_bdh"="bdh",
                 "ins_cc_beneficiario"="ced_beneficiario_bdh",
                 "cae_rinde_examen"="rinde_examen",
                 "pais_nacimiento"="pais_nace"))

ins_4<-inscritos_p15 %>%
  mutate(#Variables que se crean para identificar al colegio de procedencia:
    #------VARIABLES DEL PER2 AL PER12:
    nota_verbal=9999,
    nota_logica=9999,
    nota_abstracta=9999,
    prq_id_nace="",
    bdh="SIN REGISTRO",
    ced_beneficiario_bdh="SIN REGISTRO",
    pais_nace="SIN REGISTRO",
    ued_id="SIN REGISTRO",
    ued_nombre="SIN REGISTRO",
    ued_tipo="SIN REGISTRO",
    codigo_parroquia_ue="SIN REGISTRO",
    prd_jornada="SIN REGISTRO",
    edad=calculo_edad(usu_fecha_nac),
    sustentantes=ifelse(ins_estado_asigna_sede==1,1,0)) %>%
  mutate(domicilio=paste(ins_calle_principal,
                         ins_barrio_sector,
                         ins_num_casa,
                         sep="|")) %>%
  select(#Variables del estudiante:
    per_id,#1
    cod_parroquia_reside,#2
    usu_id,#3
    usu_cc,#4
    usu_tipo_doc,#5
    ins_sexo,#6
    usu_nombres,#7
    usu_apellidos,#8
    usu_fecha_nac,#9
    ins_autoidentificacion,#10
    ins_nacionalidad,#11
    pais_res,#12
    pais_nace,#13
    ins_tipo_discapacidad,#14
    ins_discapacidad_mayor30,#15
    ins_porcentaje_discapacidad,#16
    ins_carnet_conadis,#17
    domicilio,#18
    ins_telefono,#19
    ins_celular,#20
    usu_email,#21
    usu_estado_civil,#22
    ins_poblacion,#23
    rinde_examen,#24
    ced_beneficiario_bdh,#25
    bdh,#26
    #Variables del colegio de procedencia:
    ins_amie_escolar,#27
    ued_id,#28
    ued_nombre,#29
    ued_tipo,#30
    codigo_parroquia_ue,#31
    prd_jornada,#32
    #Variables de las calificaciones por dominios:
    nota_postula,#33
    #Variables de control:
    nota_verbal,#34
    nota_logica,#35
    nota_abstracta,#36
    prq_id_nace,#37
    edad,
    sustentantes) %>% #38
  plyr::rename(c("ins_autoidentificacion"="autoidentificacion",
                 "ins_nacionalidad"="pueblos_nacionalidades",
                 "ins_discapacidad_mayor30"="es_discapacitado",
                 "ins_tipo_discapacidad"="tipo_discapacidad",
                 "ins_porcentaje_discapacidad"="grado_discapacidad",
                 "ins_carnet_conadis"="nro_carnet_conadis",
                 "ins_telefono"="telefono",
                 "ins_celular"="movil",
                 "ins_poblacion"="escolaridad",
                 "cod_parroquia_reside"="prq_id_reside",
                 "usu_tipo_doc"="tipo_documento",
                 "ins_sexo"="sexo",
                 "usu_nombres"="nombres",
                 "usu_apellidos"="apellidos",
                 "usu_fecha_nac"="fecha_nacimiento",
                 "pais_res"="pais_reside",
                 "usu_email"="email",
                 "usu_estado_civil"="estado_civil",
                 "ins_amie_escolar"="ued_amie"))
#===============================================================================================

# Unificación de los inscritos del per2 al per15

inscritos_p2_p15<-rbind(ins_1,ins_2,ins_3,ins_4)

inscritos_p2_p15<-as.data.frame(
  lapply(inscritos_p2_p15, function(x)
    if(is.character(x))
    {
      trimws(x)
      gsub("^$","SIN REGISTRO",x)
      ifelse(is.na(x),"SIN REGISTRO",x)
      toupper(x)
    }
    else
    {
      x
    }),
  stringsAsFactors=F)

inscritos_p2_p15$periodos<-recode(inscritos_p2_p15$per_id,
                                  "2"="1er. Semestre 2012",
                                  "3"="2do. Semestre 2012",
                                  "4"="1er. Semestre 2013",
                                  "5"="2do. Semestre 2013",
                                  "6"="1er. Semestre 2014",
                                  "7"="2do. Semestre 2014",
                                  "8"="1er. Semestre 2015",
                                  "9"="2do. Semestre 2015",
                                  "10"="1er. Semestre 2016",
                                  "12"="2do. Semestre 2016",
                                  "13"="1er. Semestre 2017",
                                  "14"="2do. Semestre 2017",
                                  "15"="1er. Semestre 2018")

inscritos_p2_p15$periodos<-factor(inscritos_p2_p15$periodos,
                                  levels=c("1er. Semestre 2012",
                                           "2do. Semestre 2012",
                                           "1er. Semestre 2013",
                                           "2do. Semestre 2013",
                                           "1er. Semestre 2014",
                                           "2do. Semestre 2014",
                                           "1er. Semestre 2015",
                                           "2do. Semestre 2015",
                                           "1er. Semestre 2016",
                                           "2do. Semestre 2016",
                                           "1er. Semestre 2017",
                                           "2do. Semestre 2017",
                                           "1er. Semestre 2018"))
#===============================================================================================
# Join con las BDD de las
inscritos_p2_p15$i01_reside<-substr(inscritos_p2_p15$prq_id_reside,1,2)
inscritos_p2_p15$i02_reside<-substr(inscritos_p2_p15$prq_id_reside,1,4)
inscritos_p2_p15 <- merge(inscritos_p2_p15,zonas,by=c("i01_reside","i02_reside"),
                          all.x=T)
#===============================================================================================
# Aki en esta sección vamos a recodificar variable por variable:

inscritos_p2_p15 <- inscritos_p2_p15 %>% 
  mutate(sexo=trimws(sexo)) %>% 
  mutate(sexo=recode(sexo,
                     "F"="MUJERES",
                     "M"="HOMBRES",
                     "m"="HOMBRES",
                     "HOMBRE"="HOMBRES",
                     "MUJER"="MUJERES"),
         autoidentificacion=recode(trimws(autoidentificacion),
                                   "Afrodecendiente"="AFROECUATORIANOS",
                                   "AFRODECENDIENTE"="AFROECUATORIANOS",
                                   "AFROECUATORIANO/A"="AFROECUATORIANOS",
                                   "AFRODESCENDIENTE"="AFROECUATORIANOS",
                                   "BLANCO/A"="BLANCOS",
                                   "BLANCO"="BLANCOS",
                                   "INDÍGENA"="INDÍGENAS",
                                   "MESTIZO/A"="MESTIZOS",
                                   "MESTIZO"="MESTIZOS",
                                   "MONTUBIO/A"="MONTUBIOS",
                                   "MONTUBIO"="MONTUBIOS",
                                   "MULATO"="AFROECUATORIANOS",
                                   "MULATO/A"="AFROECUATORIANOS",
                                   "NEGRO/A"="AFROECUATORIANOS",
                                   "NEGRO"="AFROECUATORIANOS",
                                   "AFRODESCENDIENTE"="AFROECUATORIANOS",
                                   "MONTUVIO"="MONTUBIOS",
                                   "MONTUVIO/A"="MONTUBIOS",
                                   "OTRO/A"="SIN REGISTRO",
                                   "OTRO"="SIN REGISTRO")) %>% 
  mutate(pueblos_nacionalidades=trimws(gsub("NACIONALIDAD","",pueblos_nacionalidades))) %>% 
  mutate(pueblos_nacionalidades=trimws(gsub("PUEBLO","",pueblos_nacionalidades))) %>% 
  mutate(autoidentificacion=gsub("^$","SIN REGISTRO",autoidentificacion)) %>% 
  mutate(autoidentificacion=ifelse(is.na(autoidentificacion),"SIN REGISTRO",
                                   autoidentificacion)) %>% 
  mutate(pueblos_nacionalidades=gsub("^$","SIN REGISTRO",pueblos_nacionalidades)) %>% 
  mutate(pueblos_nacionalidades=recode(pueblos_nacionalidades,
                                       "AWA"="AWÁ",
                                       "COFAN"="COFÁN",
                                       "KICHWA AMAZONIA"="KICHWA",
                                       "KICHWA DE LA SIERRA"="KICHWA",
                                       "KICWA"="KICHWA",
                                       "NINGUNA"="SIN REGISTRO",
                                       "OTRO"="SIN REGISTRO",
                                       "TSACHILA"="TSÁCHILA",
                                       "WAORANI"="HUAORANI",
                                       "WARANKA"="WARANCA",
                                       "ZAPARA"="ZÁPARA",
                                       "SALASAKA"="SALASACA")) %>% 
  mutate(pueblos_nacionalidades=ifelse(is.na(pueblos_nacionalidades),
                                       "SIN REGISTRO",
                                       pueblos_nacionalidades)) %>% 
  mutate(pueblos_nacionalidades=ifelse(autoidentificacion!="INDÍGENAS", 
                                       "NO APLICA",pueblos_nacionalidades)) %>% 
  mutate(rinde_examen=trimws(rinde_examen)) %>% 
  mutate(rinde_examen=ifelse(is.na(rinde_examen),"NO RINDIERON",rinde_examen)) %>% 
  mutate(rinde_examen=recode(rinde_examen,
                             "M"="RINDIERON",
                             "N"="RINDIERON")) %>% 
  mutate(pais_reside=trimws(pais_reside)) %>% 
  mutate(pais_reside=ifelse(is.na(pais_reside),"SIN REGISTRO",
                            pais_reside)) %>% 
  mutate(pais_reside=recode(pais_reside,
                            "OTRAS NACIONES DE AMÉRICA"="SIN REGISTRO",
                            "SANTA ELENA"="SIN REGISTRO",
                            "ALEMANIA, REPÚBLICA DEMOCRÁTICA"="SIN REGISTRO",
                            "OTRO PAIS"="SIN REGISTRO",
                            "PERU"="PERÚ",
                            "CHINA REPÚBLICA POPULAR (PEKIN)"="CHINA")) %>% 
  mutate(pais_nace=trimws(pais_nace)) %>% 
  mutate(pais_nace=ifelse(is.na(pais_nace),"SIN REGISTRO",
                          pais_nace)) %>% 
  mutate(pais_nace=recode(pais_nace,
                          "CHINA (DESAMBIGUACIÓN)"="CHINA",
                          "CHINA REPÚBLICA POPULAR (PEKIN)"="CHINA",
                          "KAZAJSTÁN"="KAZAJISTÁN",
                          "DOMINICA"="REPÚBLICA DOMINICANA",
                          "DOMINICANA, REPÚBLICA"="REPÚBLICA DOMINICANA",
                          "OTRAS NACIONES DE AMÉRICA"="SIN REGISTRO",
                          "PALESTINA (REGIÓN)"="PALESTINA",
                          "PERU"="PERÚ",
                          "RUSIA, FEDERACIÓN DE (UNIÓN SOVIÉTICA)"="RUSIA",
                          "SANTA ELENA"="SIN REGISTRO")) %>% 
  mutate(es_discapacitado=trimws(es_discapacitado)) %>% 
  mutate(tipo_discapacidad=trimws(tipo_discapacidad)) %>% 
  mutate(tipo_discapacidad=ifelse(is.na(tipo_discapacidad),
                                  "NO APLICA",
                                  tipo_discapacidad)) %>% 
  mutate(tipo_discapacidad=recode(tipo_discapacidad,
                                  "FISICA"="FÍSICA",
                                  "FASICA"="FÍSICA",
                                  "FA\u008dSICA"="FÍSICA",
                                  "PSICOLOGICO"="PSICOSOCIAL",
                                  "PSICOLÓGICA"="PSICOSOCIAL",
                                  "PSICOLOGICO"="PSICOSOCIAL",
                                  "SIN REGISTRO"="NO PRESENTAN DISCAPACIDAD",
                                  "NO APLICA"="NO PRESENTAN DISCAPACIDAD")) %>% 
  mutate(es_discapacitado=ifelse(tipo_discapacidad=="NO PRESENTAN DISCAPACIDAD",
                                 "NO PRESENTAN DISCAPACIDAD",
                                 "PRESENTAN DISCAPACIDAD")) %>% 
  mutate(prq_id_reside=trimws(prq_id_reside)) %>% 
  #mutate(prq_id_reside=ifelse(is.na(prq_id_reside),"SIN REGISTRO",prq_id_reside)) %>% 
  mutate(prq_id_nace=trimws(prq_id_nace)) %>% 
  #mutate(prq_id_nace=ifelse(is.na(prq_id_nace),"SIN REGISTRO",prq_id_nace)) %>% 
  mutate(grado_discapacidad=trimws(grado_discapacidad)) %>% 
  mutate(grado_discapacidad=ifelse(is.na(grado_discapacidad),
                                   "NO PRESENTAN DISCAPACIDAD",
                                   grado_discapacidad)) %>% 
  mutate(grado_discapacidad=ifelse(grado_discapacidad=="LEVE",
                                   30,
                                   ifelse(grado_discapacidad=="MODERADA",
                                   50,
                                   ifelse(grado_discapacidad=="MODERADO",
                                   50,
                                   ifelse(grado_discapacidad=="GRAVE",
                                   75,
                                   ifelse(grado_discapacidad=="MUY GRAVE",
                                   85,
                                   ifelse(grado_discapacidad=="SEVERA",
                                   85,
                                   ifelse(grado_discapacidad=="NO PRESENTA GRADO DISCAPACIDAD",
                                   "NO PRESENTAN DISCAPACIDAD",
                                   grado_discapacidad)))))))) %>% 
  mutate(grado_discapacidad=ifelse(es_discapacitado=="NO PRESENTAN DISCAPACIDAD",
                                   "NO PRESENTAN DISCAPACIDAD",
                                   grado_discapacidad)) %>%
  mutate(grado_discapacidad_aux=ifelse(grado_discapacidad=="NO PRESENTAN DISCAPACIDAD",
                                       "",grado_discapacidad)) %>% 
  mutate(grado_discapacidad_aux=as.numeric(as.character(grado_discapacidad_aux))) %>%   
  mutate(categoria_discapacidad=ifelse(grado_discapacidad_aux<=49,
                                       "LEVE",
                                       ifelse(grado_discapacidad_aux>=50 & 
                                              grado_discapacidad_aux<=74,
                                       "MODERADO",
                                       ifelse(grado_discapacidad_aux>=75 &
                                              grado_discapacidad_aux<=84,
                                       "GRAVE",
                                       ifelse(grado_discapacidad_aux>=85,
                                       "MUY GRAVE",
                                              grado_discapacidad_aux))))) %>% 
  mutate(categoria_discapacidad=ifelse(is.na(categoria_discapacidad),
                                       "NO PRESENTAN DISCAPACIDAD",
                                       categoria_discapacidad)) %>% 
  mutate(nro_carnet_conadis=ifelse(is.na(nro_carnet_conadis),
                                   "SIN REGISTRO",
                                   nro_carnet_conadis)) %>% 
  mutate(domicilio=trimws(domicilio)) %>% 
  mutate(domicilio=ifelse(domicilio=="NA|NA|NA|NA|NA",
                          "SIN REGISTRO",
                          domicilio)) %>% 
  mutate(domicilio=ifelse(is.na(domicilio),
                          "SIN REGISTRO",
                          domicilio)) %>% 
  mutate(domicilio=gsub('\\|NA\\|', '. ',domicilio)) %>% 
  mutate(domicilio=gsub('\\|', '. ',domicilio)) %>% 
  mutate(domicilio=recode(domicilio,
                          "SIN REGISTRO. SIN REGISTRO. SIN REGISTRO"="SIN REGISTRO")) %>% 
  mutate(bdh=trimws(bdh)) %>% 
  mutate(bdh=recode(bdh,"SÍ"="RECIBEN","S"="RECIBEN",
                    "N"="NO RECIBEN")) %>% 
  mutate(bdh=ifelse(bdh!=c("RECIBEN","NO RECIBEN"),"SIN REGISTRO",bdh)) %>% 
  mutate(bdh=ifelse(is.na(bdh),"SIN REGISTRO",bdh)) %>% 
  mutate(email=trimws(email)) %>% 
  mutate(email=tolower(email)) %>% 
  mutate(email=ifelse(is.na(email),"SIN REGISTRO",email)) %>% 
  mutate(ued_nombre=trimws(ued_nombre)) %>% 
  mutate(ued_nombre=ifelse(is.na(ued_nombre),"SIN REGISTRO",ued_nombre)) %>% 
  mutate(ued_tipo=trimws(ued_tipo)) %>% 
  mutate(ued_tipo=ifelse(is.na(ued_tipo),"SIN REGISTRO",ued_tipo)) %>% 
  mutate(escolaridad=trimws(escolaridad)) %>% 
  mutate(escolaridad=ifelse(is.na(escolaridad),"SIN REGISTRO",escolaridad)) %>%
  mutate(escolaridad=recode(escolaridad,"ESCOLAR"="ESCOLARES",
                            "NO ESCOLAR"="NO ESCOLARES")) %>% 
  mutate(ued_id=trimws(ued_id)) %>% 
  mutate(ued_id=ifelse(is.na(ued_id),"SIN REGISTRO",ued_id)) %>% 
  mutate(ued_amie=trimws(ued_amie)) %>% 
  mutate(ued_amie=ifelse(is.na(ued_amie),"SIN REGISTRO",ued_amie)) %>% 
  mutate(prd_jornada=trimws(prd_jornada)) %>% 
  mutate(prd_jornada=ifelse(is.na(prd_jornada),"SIN REGISTRO",prd_jornada)) %>% 
  mutate(codigo_parroquia_ue=trimws(codigo_parroquia_ue)) %>% 
  mutate(codigo_parroquia_ue=ifelse(is.na(codigo_parroquia_ue),
                                    "SIN REGISTRO",codigo_parroquia_ue)) %>% 
  mutate(estado_civil=trimws(estado_civil)) %>% 
  mutate(estado_civil=ifelse(is.na(estado_civil),
                             "SIN REGISTRO",estado_civil)) %>% 
  mutate(estado_civil=recode(estado_civil,
                             "C"="CASADOS",
                             "CASADO/A"="CASADOS",
                             "D"="DIVORCIADOS",
                             "DIVORCIADO/A"="DIVORCIADOS",
                             "S"="SOLTEROS",
                             "SOLTERO/A"="SOLTEROS",
                             "U"="EN UNIÓN LIBRE",
                             "UNION LIBRE"="EN UNIÓN LIBRE",
                             "V"="VIUDOS",
                             "VIUDO/A"="VIUDOS")) %>% 
  mutate(telefono=trimws(telefono)) %>% 
  mutate(telefono=ifelse(is.na(telefono),"SIN REGISTRO",telefono)) %>% 
  mutate(movil=trimws(movil)) %>% 
  mutate(movil=ifelse(is.na(movil),"SIN REGISTRO",movil)) %>% 
  mutate(prq_id_reside=trimws(prq_id_reside)) %>%   
  mutate(prq_id_reside=recode(prq_id_reside,"-"="")) %>% 
  mutate(prq_id_nace=trimws(prq_id_nace)) %>%   
  mutate(prq_id_nace=recode(prq_id_nace,"-"="")) %>% 
  select(-i01_reside,-i02_reside) %>% 
  mutate(sustentantes=ifelse(is.na(sustentantes),0,sustentantes))
#===============================================================================================
# Almacenamiento a la BDD PostgreSQL senescyt_bi:
dbWriteTable(senescyt_bi,"inscritos_totales_p2_p15",
             inscritos_p2_p15,overwrite=T,row.names=F)
#===============================================================================================