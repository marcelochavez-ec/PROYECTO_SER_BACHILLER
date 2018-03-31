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
# #============================================================================================================
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
    }
    else
    {
      x
    }),
  stringsAsFactors=F)

#============================================================================================================
# SELECCIÓN DE VARIABLES:

ins_1<-inscritos_1 %>% 
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
                    nota_postula) %>% #31
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
                                 "rinde_enes"="rinde_examen")) %>% 
                  mutate(#Variables que se crean para identificar al colegio de procedencia:
                    edad=calculo_edad(fecha_nacimiento),#32
                    #------VARIABLES DEL PER13 AL PER14:
                    escolaridad="SIN REGISTRO",#33
                    ued_id="SIN REGISTRO",#34
                    ued_amie="SIN REGISTRO",#35
                    prd_jornada="SIN REGISTRO",#36
                    codigo_parroquia_ue="SIN REGISTRO",#37
                    estado_civil="SIN REGISTRO")#38


ins_2<-inscritos_2 %>%
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
                    nota_postula) %>% #31
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
                                 "rinde_enes"="rinde_examen")) %>% 
                  mutate(#Variables que se crean para identificar al colegio de procedencia:
                    edad=calculo_edad(fecha_nacimiento),#32
                    #------VARIABLES DEL PER13 AL PER14:
                    escolaridad="SIN REGISTRO",#33
                    ued_id="SIN REGISTRO",#34
                    ued_amie="SIN REGISTRO",#35
                    prd_jornada="SIN REGISTRO",#36
                    codigo_parroquia_ue="SIN REGISTRO",#37
                    estado_civil="SIN REGISTRO")#38


ins_3<-inscritos_3 %>% 
       mutate(#Variables que se crean para identificar al colegio de procedencia:
       #------VARIABLES DEL PER2 AL PER12:
       nota_verbal="",
       nota_logica="",
       nota_abstracta="",
       prq_id_nace="SIN REGISTRO",
       edad=calculo_edad(fecha_nacimiento),
       es_discapacitado="SIN REGISTRO") %>% 
       mutate(domicilio=paste(ins_calle_principal,
                              ins_calle_secundaria,
                              ins_barrio_sector,
                              ins_referencia,
                              ins_num_casa,
                              sep=" | ")) %>%
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
                  es_discapacitado) %>% #38
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
              nota_verbal="",
              nota_logica="",
              nota_abstracta="",
              prq_id_nace="SIN REGISTRO",
              bdh="SIN REGISTRO",
              ced_beneficiario_bdh="SIN REGISTRO",
              pais_nace="SIN REGISTRO",
              ued_id="SIN REGISTRO",
              ued_nombre="SIN REGISTRO",
              ued_tipo="SIN REGISTRO",
              codigo_parroquia_ue="SIN REGISTRO",
              prd_jornada="SIN REGISTRO",
              edad=calculo_edad(usu_fecha_nac)) %>%
              mutate(domicilio=paste(ins_calle_principal,
                                     ins_barrio_sector,
                                     ins_num_casa,
                                     sep=" | ")) %>%
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
                     edad) %>% #38
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

#===============================================================================================
# Aki en esta sección vamos a recodificar variable por variable:

tipo_documento<-data.frame(table(inscritos_p2_p15$tipo_documento))
sexo <- data.frame(table(inscritos_p2_p15$sexo))
autoidentificacion <- data.frame(table(inscritos_p2_p15$autoidentificacion))
pueblos_nacionalidades <- data.frame(table(inscritos_p2_p15$pueblos_nacionalidades))
rinde_examen <- data.frame(table(inscritos_p2_p15$rinde_examen))
pais_reside <- data.frame(table(inscritos_p2_p15$pais_reside))
pais_nace <- data.frame(table(inscritos_p2_p15$pais_nace))
es_discapacitado <- data.frame(table(inscritos_p2_p15$es_discapacitado))

prueba_inscritos <- inscritos_p2_p15 %>% 
                    mutate(sexo=recode(sexo,
                                       "F"="MUJERES",
                                       "M"="HOMBRES",
                                       "m"="HOMBRES",
                                       "HOMBRE"="HOMBRES",
                                       "MUJER"="MUJERES"),
                          autoidentificacion=recode(toupper(autoidentificacion),
                                             " "="SIN REGISTRO",
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
                                             "OTRO"="SIN REGISTRO"))






