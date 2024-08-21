# Manipulation de données
library(dplyr)
library(tidyr)
library(stringr)
library(arrow)
library(hutils)
library(purrr)
options(arrow.use_threads = TRUE)
arrow::set_cpu_count(parallel::detectCores() %/% 4)

# Cartographie
library(sf)
library(ggplot2)

# Chemin
path_input <- "Z:/Memoire/dv3f_memoire/Inputs_climat/"
path_base_intermediaire <- "Z:/Memoire/dv3f_memoire/Bases_intermediaires/"
path_outputs <- "Z:/Memoire/dv3f_memoire/Outputs/"

rep_donnees <-  "P:/PSAR_SL/SL45/Lot 3/202401/bases/comptages"

#############################
###   Objectif du script  ###
#############################

# Réaliser  le travail préparatoire sur la base des prix (DV3F), en ajoutant les variables
# nécessaires proposées par le PSAR et en executant les filtres nécessaires
# Dédicace au PSAR

#################################
###  1. Chargement des bases  ###
#################################

base_totale <- readRDS('P:/PSAR_SL/SL45/Lot 3/202401/bases/DV3F/Base_DV3F_V2024-1-filtre.rds')
arrow::write_parquet(base_totale, 'Z:/DV3F/base_totale')

##############################################################
###  2. Enrichissement et filtres choisis par ma personne  ###
##############################################################
base_totale <- arrow::open_dataset('Z:/DV3F/base_totale')
# 2.1 Ajout des saisons
base_totale <- base_totale |>
  mutate(saison =  case_when(
    lubridate::month(datemut) %in% c(1, 2, 3) ~ paste0(anneemut, "-01-01"),
    lubridate::month(datemut) %in% c(4, 5, 6) ~ paste0(anneemut, "-04-01"),
    lubridate::month(datemut) %in% c(7, 8, 9) ~ paste0(anneemut, "-07-01"),
    lubridate::month(datemut) %in% c(10, 11, 12) ~ paste0(anneemut, "-10-01")
  ))|>
  compute()

## Nombre d'observations par type de bien (2018-2022)
years <- c(2018:2022)
compte_par_type <- base_totale |>
  filter(anneemut %in% years)|>
  compute()|>
  to_duckdb() |>
  group_by(codtypbien) |>
  summarize(nbr_mutations = n())|>
  to_arrow()|>
  collect()

# Les seules maisons disponibles sont : 1112 (maison unique, 2 à 4 ans), 1113 (maison ancienne)
# en vente d'une seule maison à la fois.

# 2.2. Filtres : maisons, champ, prix extrêmes
base_maisons <- base_totale |>
  filter(codtypbien == '1112' | codtypbien == '1113')|> #maisons uniquement
  filter(!(codgeo %in% c(29083, 29155))) |> #On retire l'Ile de Sein et Ouessant, non localisés dans nos mailles
  filter(!str_detect(codgeo, "^97")) |> #On retire l'outre-mer
  filter(prix_extreme=='non') |>
  compute()

arrow::write_parquet(base_maisons, 'Z:/DV3F/base_maisons')

# 2.3 Ajout du RGA
base_maisons <- arrow::open_dataset('Z:/DV3F/base_maisons')
maisons_sf <- base_maisons |> 
  select(geometry, idmutation, dep)|>
  filter(!is.na(geometry)) |> #2621 mutations sans localisation (apparts compris)
  collect()|>
  sf::st_as_sf(wkt = "geometry",
               crs = 2154) #aucune certitude de la projection !! 
gc()
  
zonage_rga <- sf::st_read('X:/HAB-immoclimat/Data/rga_et_swi/exposition_rga/ExpoArgile_Fxx_L93.shp') %>% 
  select(-c("DPT"))%>% 
  sf::st_transform(2154)
gc()

unique_deps <- unique(maisons_sf$dep)
existing_files <- list.files(path_base_intermediaire, pattern = "\\.rds$", full.names = TRUE)
existing_deps <- basename(existing_files)
existing_deps <- sub("\\.rds$", "", existing_deps)

# On corrige les géométries : 
maisons_sf <- st_make_valid(st_cast(maisons_sf, 'POINT')) %>% 
  sf::st_transform(crs=2154)

# Filter out departments for which there is no saved result
remaining_deps <- setdiff(unique_deps, existing_deps)

#remaining_deps <- rev(remaining_deps) # permet d'avoir plusieurs sessions réalisant le calcul en parallèle

for (dep in remaining_deps) {
  message("Processing dep: ", dep)
  # Compute intersection at the departement level
  base_sf_dep <- maisons_sf %>% filter(dep == !!dep)
  intersection_result <- sf::st_intersection(base_sf_dep, zonage_rga)
  saveRDS(intersection_result, paste0(path_base_intermediaire, dep, ".rds"))
  # Clear memory
  rm(base_sf_dep)
  rm(intersection_result)
  gc()
}

# On recrée un df complet
rds_files <- list.files(path_base_intermediaire, pattern = "^.{1,2}\\.rds$", full.names = TRUE)
df_list <- list()
for (file in rds_files) {
  message("Loading file: ", file)
  df <- readRDS(file)
  df_list <- append(df_list, list(df))
}
combined_df <- bind_rows(df_list)

# Left join
base_maisons <- arrow::open_dataset('Z:/DV3F/base_maisons')
carte_rga <- combined_df |>
  sf::st_drop_geometry()|>
  select(-c("dep"))|>
  arrow::as_arrow_table()

base_maisons_avec_RGA <- base_maisons|>
  left_join(carte_rga, by = 'idmutation')|>
  compute()

base_maisons_avec_RGA |> 
  to_duckdb()|>
  group_by(ALEA)|>
  summarize(number = n(),
          share = n() / denominator)|>
  to_arrow()|>
  collect()

# 55% des transactions en RGA moyen ou fort : très proche des 52% des maisons individuelles

arrow::write_parquet(base_maisons_avec_RGA, 'Z:/DV3F/base_maisons_avec_RGA')

#########################################################
###  3. Ajouts des bases externes (dédicace au PSAR)  ###
#########################################################

base_maisons <- arrow::open_dataset('Z:/DV3F/base_maisons_avec_RGA') 

## 3.1 Base au carreau de 200m
### POUR REFAIRE CE LEFT JOIN, ALLER A LA BALISE A
path_region <- 'P:/PSAR_SL/SL45/Lot 3/202401/bases/carreaux_200/regions/'

# Pour chaque région, on localise les transactions, et on crée un fichier .RDS
regions <- base_maisons |> 
  select(reg)|>
  distinct()|> 
  collect()|>
  pull()

maisons_sf <- base_maisons |>
  select(geometry, idmutation, reg)|>
  filter(!is.na(geometry)) |> #2621 mutations sans localisation (apparts compris)
  collect()|>
  sf::st_as_sf(wkt = "geometry",
               crs = 2154)
gc()

for (reg in regions) {
  message("Processing reg: ", reg)
  # Choix de la base carroyée
  all_dirs <- list.dirs(path_region, full.names = TRUE, recursive = FALSE)
  region_dir <- all_dirs[grep(paste0("^", reg), basename(all_dirs))]
  base_carreaux_200 <- readRDS(paste0(region_dir,'/base_carreaux_200.rds'))
  # Choix de la base transaction
  maisons_region_sf <- maisons_sf %>% filter(reg == !!reg)
  gc()
  # Calcul de l'intersection
  intersection <- st_intersection(x = maisons_region_sf, y =base_carreaux_200)
  saveRDS(intersection, paste0(path_base_intermediaire, 'intersection_200m_', reg, ".rds"))
  # Clear memory
  rm(maisons_region_sf)
  rm(intersection)
  rm(base_carreaux_200)
  gc()
}

# On reforme la base maisons, désormais enrichie des infos supplémentaires
intersections_list <- list.files(path_base_intermediaire, pattern = "intersection_200m_.*\\.rds", full.names = TRUE) %>%
  purrr::map(readRDS) %>%
  purrr::list_rbind() %>% 
  sf::st_drop_geometry() %>% 
  select(-c('geometry', 'reg', 'reg.1', 'code', 'libelle')) %>% 
  distinct(idmutation, .keep_all = TRUE) %>% # On traite 136 duplications
  arrow::as_arrow_table()

base_maisons_enrichie <- base_maisons |> 
  left_join(intersections_list, by = 'idmutation') |>
  compute()
gc()
## On traite les 7056 non identifiés : 4984 ont une geometrie, 2072 n'en ont pas
non_identified <- base_maisons_enrichie |> 
  filter(is.na(id_carr_1k)) |> 
  select(idmutation) |>
  collect() |>
  pull()

non_identified_sf <- maisons_sf |> filter(idmutation %in% non_identified)

for (reg in regions) {
  message("Processing reg: ", reg)
  # Choix de la base carroyée
  all_dirs <- list.dirs(path_region, full.names = TRUE, recursive = FALSE)
  region_dir <- all_dirs[grep(paste0("^", reg), basename(all_dirs))]
  base_carreaux_200 <- readRDS(paste0(region_dir,'/base_carreaux_200.rds'))
  gc()
  # Calcul de l'intersection
  intersection <- st_intersection(x = non_identified_sf, y =base_carreaux_200)
  saveRDS(intersection, paste0(path_base_intermediaire, 'non_identified_', reg, ".rds"))
  # Clear memory
  rm(intersection)
  rm(base_carreaux_200)
  gc()
}
# ça nous permet de récuperer environ 900 points situés dans une limite de région
second_intersection <- list.files(path_base_intermediaire, pattern = "non_identified_.*\\.rds", full.names = TRUE) %>%
  purrr::map(readRDS) %>%
  purrr::list_rbind() %>% 
  sf::st_drop_geometry() %>% 
  select(-c('geometry', 'reg', 'reg.1', 'code', 'libelle')) %>% 
  distinct(idmutation, .keep_all = TRUE) %>% 
  arrow::as_arrow_table()

second_intersection_enrichie <- second_intersection |> 
  left_join(base_maisons, by = 'idmutation') |>
  collect()

ids <- second_intersection_enrichie |> 
  filter(!is.na(id_carr_1k)) |> 
  select(idmutation) |>
  collect() |>
  pull()

base_maisons_enrichie <- base_maisons_enrichie |> 
  filter(!(idmutation %in% ids)) %>% 
  collect()|>
  rbind(second_intersection_enrichie)

### BALISE : reprendre ici lorsqu'on ne veut pas recalculer les intersections
intersections_list <- list.files(path_base_intermediaire, pattern = "intersection_200m_.*\\.rds", full.names = TRUE) %>%
  purrr::map(readRDS) %>%
  purrr::list_rbind() %>% 
  sf::st_drop_geometry() %>% 
  select(-c('geometry', 'reg', 'reg.1', 'code', 'libelle')) %>% 
  distinct(idmutation, .keep_all = TRUE) %>% # On traite 136 duplications
  arrow::as_arrow_table()

second_intersection <- list.files(path_base_intermediaire, pattern = "non_identified_.*\\.rds", full.names = TRUE) %>%
  purrr::map(readRDS) %>%
  purrr::list_rbind() %>% 
  sf::st_drop_geometry() %>% 
  select(-c('geometry', 'reg', 'reg.1', 'code', 'libelle')) %>% 
  distinct(idmutation, .keep_all = TRUE) %>% 
  arrow::as_arrow_table()

intersection <- intersections_list |>
  rbind(second_intersection)|>
  compute()

base_maisons_enrichie <- base_maisons |> 
  left_join(intersection, by = 'idmutation') |>
  compute()
gc()

arrow::write_parquet(base_maisons_enrichie, paste0(path_base_intermediaire, 'bases_maisons_enrichie'))

## 3.2 Bases au niveau de l'iris
iris_sf <- readRDS('P:/PSAR_SL/SL45/Lot 3/202401/bases/Iris/base_iris_geo.rds') %>% 
  sf::st_transform(crs=2154)

maisons_sf <- base_maisons |>
  select(geometry, idmutation, reg)|>
  filter(!is.na(geometry)) |> #2621 mutations sans localisation (apparts compris)
  collect()|>
  sf::st_as_sf(wkt = "geometry",
               crs = 2154)

intersection_iris <- sf::st_intersection(maisons_sf, iris_sf)

## 3.3 Bases au niveau de la commune
base_communale <- readRDS('P:/PSAR_SL/SL45/Lot 3/202401/bases/communes/base_communale.rds')

intersection_iris_avec_commune <- base_maisons |>
  select(codgeo, idmutation) |> 
  collect() |>
  left_join(base_communale %>% select(-c('LIB_COM', starts_with('risque'))), by = c('codgeo'='DEPCOM')) |>
  left_join(intersection_iris, by = 'idmutation') 

# Finalement, on perd 16K à l'iris, et 139 à la commune (car communes fusionnées, ici les 60694, 85212)
# Pour les 16K à l'iris, c'est à cause de l'absence de géometries
test <- intersection_iris_avec_commune |> filter(is.na(CODE_IRIS) & !sf::st_is_empty(geometry))

intersection_iris_avec_commune <- intersection_iris_avec_commune |> 
  select(-c('geometry', 'codgeo')) |>
  arrow::as_arrow_table()

# On forme la base finale : 
base_maisons_enrichie <- arrow::open_dataset(paste0(path_base_intermediaire, 'bases_maisons_enrichie'))

base_maisons_finale <- base_maisons_enrichie |>
  left_join(intersection_iris_avec_commune,by = 'idmutation')|>
  compute()

arrow::write_parquet(base_maisons_finale, paste0(path_base_intermediaire, 'bases_maisons_enrichie'))

###########################################################
###  4. Reformulation des variables pour la regression  ###
###########################################################

base_maisons <- arrow::open_dataset(paste0(path_base_intermediaire, 'bases_maisons_enrichie'))

base_maisons_avec_reformulations <- base_maisons|>
  mutate(
    Trois_pieces_et_moins=ifelse(nbpprinc<=3,1,0),   # indicatrices du nombre de pièces
    Quatre_pieces=ifelse(nbpprinc==4,1,0),
    Cinq_pieces=ifelse(nbpprinc==5,1,0),
    Six_pieces=ifelse(nbpprinc==6,1,0),
    Sept_pieces_et_plus=ifelse(nbpprinc>=7,1,0),
    
    Recent=ifelse(anciennete=="recent",1,0),  # indicatrices de la période de construction
    P_1914=ifelse(periodecst=="< 1914",1,0),
    P1914_1944=ifelse(periodecst=="1914-1944",1,0),
    P1945_1960=ifelse(periodecst=="1945-1960",1,0),
    P1961_1974=ifelse(periodecst=="1961-1974",1,0),
    P1975_1989=ifelse(periodecst=="1975-1989",1,0),
    P1990_2012=ifelse(anciennete!="recent" & periodecst=="1990-2012",1,0),
    P2013_=ifelse(anciennete!="recent" & periodecst==">= 2013",1,0),
    
    Un_niveau=ifelse(ffnbetage==0,1,0),  # indicatrices de nombre de niveaux
    Deux_niveaux_et_plus=ifelse(ffnbetage>=1,1,0),
    
    Sans_SdB=ifelse(ffnbpsea==0,1,0),    # indicatrices du nombre de salles de bain
    Une_SdB=ifelse(ffnbpsea==1,1,0),
    Deux_SdB_et_plus=ifelse(ffnbpsea>=2,1,0),
    
    Sans_garage=ifelse(ffnbpgarag==0,1,0),        # indicatrices du nombre de garages
    Un_garage=ifelse(ffnbpgarag==1,1,0),
    Deux_garages_et_plus=ifelse(ffnbpgarag>=2,1,0),
    
    Terrasse=ifelse(ffnbpterra>=1,1,0),          # présence d'une terrasse
    Piscine=ifelse(ffnbppisci>=1,1,0),           # présence d'une piscine
    Autre_dependance=ifelse(ffnbpaut>=1,1,0),     # présence d'autres dépendances (cave, grenier, etc...)
    
    Surface_parcelle=as.numeric(ffsparc),
    
    metro_tram=ifelse(is.na(distance_metro_tram),3,as.numeric(distance_metro_tram)), # indicatrices de distance au tram ou au métro
    Metro_tram_300m=ifelse(metro_tram<=0.3,1,0),
    Metro_tram_300m_600m=ifelse(metro_tram>0.3 & metro_tram<=0.6,1,0),
    Metro_tram_600m_1k=ifelse(metro_tram>0.6 & metro_tram<=1,1,0),
    Metro_tram_p1k_ou_non=ifelse(metro_tram >1,1,0),
    
    Gare=ifelse(is.na(distance_gare),20,as.numeric(distance_gare)), # indicatrices de distance à la gare la plus proche
    Gare_3km=ifelse(Gare<=3,1,0),
    Gare_3km_5km=ifelse(Gare>3 & Gare<=5,1,0),
    Gare_5km_7km=ifelse(Gare>5 & Gare<=7,1,0),
    Gare_7km_10km=ifelse(Gare>7 & Gare<=10,1,0),
    Gare_p10km=ifelse(Gare>10,1,0),
    Gare_500m=ifelse(Gare<=0.5,1,0),
    
    sevesosh=ifelse(is.na(distance_seveso_SH),20,as.numeric(distance_seveso_SH)), # indicatrices de distance à un établissement Seveso seuil haut
    Sevesosh_1km=ifelse(sevesosh<=1,1,0),
    Sevesosh_1km_10km=ifelse(sevesosh>1 & sevesosh<=10,1,0),
    Sevesosh_p10km=ifelse(sevesosh>10,1,0),
    
    QPV=ifelse(is.na(distance_QPV),20,as.numeric(distance_QPV)), # indicatrices d'appartenance ou de distance à un QPV
    Appartenance_QPV=ifelse(QPV<=0.1,1,0),
    QPV_500m=ifelse(QPV>0.1 & QPV<=0.5,1,0),
    QPV_p500m_ou_non=ifelse(QPV>0.5,1,0),
    
    PEB_A=ifelse(is.na(appartenance_peb_A),0,appartenance_peb_A),    # indicatrices d'appartenance à une zone d'un plan d'exposition au bruit (aéroport)
    PEB_B=ifelse(is.na(appartenance_peb_B),0,appartenance_peb_B),
    PEB_C=ifelse(is.na(appartenance_peb_C),0,appartenance_peb_C),
    PEB_D=ifelse(is.na(appartenance_peb_D),0,appartenance_peb_D),
    
    Cote=ifelse(is.na(Distance_cote),2,as.numeric(Distance_cote)), # indicatrices de distance à la cote
    cote_300m=ifelse(Cote<=0.3,1,0),
    cote_300m_1km=ifelse(Cote>0.3 & Cote<=1,1,0),
    cote_p1km=ifelse(Cote>1,1,0),
    
    Tourist_tres_eleve=ifelse(taux_fonc_tour>250,1,0),
    Tourist_eleve=ifelse(taux_fonc_tour>50 & taux_fonc_tour<=250,1,0),  # indicatrices de distance à la gare la plus proche
    Tourist_faible=ifelse(taux_fonc_tour<=50,1,0) 
  )|>
  select(-c('reg.x', 'Cote','QPV','sevesosh','Gare','metro_tram'))|>
  rename(Surface_batie=sbati,
         Valeur_fonciere=valeurfonc,
         reg = reg.y
  )|>
  collect()|>
  labelled::set_variable_labels(Trois_pieces_et_moins="Trois pièces et moins",
                                Quatre_pieces="Quatre pièces",
                                Cinq_pieces="Cinq pièces",
                                Six_pieces="Six pièces et plus",
                                Sept_pieces_et_plus="Sept pièces et plus",
                                Recent="Récent",
                                P_1914="Construit avant 1914",
                                P1914_1944="Construit entre 1914 et 1944",
                                P1945_1960="Construit entre 1945 et 1960",
                                P1961_1974="Construit entre 1961 et 1975",
                                P1975_1989="Construit entre 1975 et 1989",
                                P1990_2012="Construit entre 1990 et 2012",
                                P2013_="Construit depuis 2013 (non récent)",
                                Un_niveau="Un seul niveau",
                                Deux_niveaux_et_plus="Deux niveaux et plus",
                                Sans_SdB="Sans salle de bain",
                                Une_SdB="Une salle de bain",
                                Deux_SdB_et_plus="Deux salles de bain et plus",
                                Sans_garage="Sans garage",
                                Un_garage="Un garage",
                                Deux_garages_et_plus="Deux garages et plus",
                                Terrasse="Une terrasse",
                                Piscine="Une piscine",
                                Autre_dependance="Autres dépendances (cellier,cave,...)",
                                
                                Metro_tram_300m="Arrêt tram ou station de métro moins 300m",
                                Metro_tram_300m_600m="Arrêt tram ou station de métro entre 300m et 600m",
                                Metro_tram_600m_1k="Arrêt tram ou station de métro entre 600m et 1km",
                                Metro_tram_p1k_ou_non="Arrêt tram ou station de métro plus 1km ou sans tram / métro",
                                
                                Gare_3km="Gare moins 3km",
                                Gare_3km_5km="Gare entre 3km et 5km",
                                Gare_5km_7km="Gare entre 5km et 7km",
                                Gare_7km_10km="Gare entre 7km et 10km",
                                Gare_p10km="Gare plus 10km",
                                Gare_500m="Gare moins 500m",
                                
                                Sevesosh_1km="Etablissement Seveso SH moins 1km",
                                Sevesosh_1km_10km="Etablissement Seveso SH entre 1km et 10km",
                                Sevesosh_p10km="Etablissement Seveso SH plus 10km",
                                
                                Appartenance_QPV="Appartenance à un QPV",
                                QPV_500m="QPV à moins de 500m",
                                QPV_p500m_ou_non="QPV à plus 500m ou pas de QPV",
                                
                                PEB_A="Appartenance zone A Plan Exposition Bruit",
                                PEB_B="Appartenance zone B Plan Exposition Bruit",
                                PEB_C="Appartenance zone C Plan Exposition Bruit",
                                PEB_D="Appartenance zone D Plan Exposition Bruit",
                                
                                cote_300m="Côte moins 300m",
                                cote_300m_1km="Cote entre 300 et 1km",
                                cote_p1km="cote à plus de 1km",
                                
                                Tourist_tres_eleve="Taux fonction touristique trés élevé",
                                Tourist_eleve="Taux de fonction touristique élevé",
                                Tourist_faible="Taux de fonction touristique classique")


arrow::write_parquet(base_maisons_avec_reformulations, paste0(path_base_intermediaire, 'bases_maisons_enrichie'))