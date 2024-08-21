# Traitement de bases
library(dplyr)
library(readr)
library(lubridate)
library(tidyr)
library(readxl)
library(purrr)

# Cartographie
library(sf)
library(ggplot2)
library(btb)

# Econométrie
library(stargazer)
library(lmtest)
library(sandwich)
library(plm)

# Graphiques 
library(ggbreak)
library(ggtext)
library(scales)

path_input <- "Z:/Memoire/dv3f_memoire/Inputs_climat/"
path_outputs <- "Z:/Memoire/dv3f_memoire/Outputs/"

#############################
###   Objectif du script  ###
#############################

# Réaliser tout le travail préparatoire harmonisant les critères répliqués, les reconnaissances réelles, 
# sur les saisons pour lesquelles DV3F est disponible.

#######################################################
###   1. Réplication des critères météorologiques   ###
#######################################################

#### 1.1 Chargement des tables de SWI ####

csv_files <- list.files(paste0(path_input, "SWI_uniforme/"), pattern = "\\.csv$", full.names = TRUE)
data_frames <- list()
for (file in csv_files) {
  data_frames[[file]] <- read.delim(file, sep = ";")
}
swi_historique <- do.call(rbind, data_frames)
rm(data_frames, csv_files)

# Création d'une base utilisable 
row.names(swi_historique) <- NULL
swi_historique <- swi_historique %>% 
  dplyr::rename(maille_safran = NUMERO,
                x = LAMBX,
                y = LAMBY, 
                SWI_mens3 =SWI_UNIF_MENS3) %>% 
  dplyr::mutate(annee = substr(DATE,1,4),
                mois = substr(DATE, 5,6)) %>% 
  select(-DATE)

arrow::write_parquet(
  x = swi_historique,
  sink = paste0(paste0(path_input, "SWI_uniforme/"), "swi_historique")
)
rm(swi_historique)

#### 1.2 Calcul des critières météorologiques (2019 étendu, 2024) par maille ####

# Selon la circulaire n°INTE1911312C du 10/05/2019, une sécheresse exceptionnelle est définie comme 
# ayant une période de retour d'au moins 25 ans. En pratique, cela requiert un SWI mensuel étant le 
# plus faible ou le deuxième plus faible sur le même mois dans les 50 dernières années, pour au moins
# un des trois mois composant la saison. 

# La formule de la période de retour est : (nbr d'observations + 1)/rang, ce qui donne 51 ans pour 
# la sécheresse la plus forte, 25.5 pour la 2e, 17 pour la 3e, 12,5 pour la 4e, 10,2 pour la 5e. 
# La nouvelle circulaire n°IOME2322937C du 29/04/2024 considère désormais qu'une durée de retour 
# supérieure à 10 ans suffit à obtenir la reconnaissance de catastrophe naturelle, ce qui témoigne de
# la possibilité d'avoir des sinistres pour les valeurs inférieures. 


### 1.2.1. Calcul des sécheresses et des rangs (méthode de 2019)
## Première observation en 1969 : avec la méthode réelle (2 plus grandes périodes de retour sur 50 ans), on n'a alors que 2018-2022
swi_historique <- arrow::read_parquet(paste0(path_input, "SWI_uniforme/swi_historique"))
swi_historique %>% summarize(min_year = min(annee), max_year = max(annee)) 

## Faire un unique calcul dynamique de rang serait plus élégant, mais je n'y parviens pas -> nous
## optons donc pour un calcul par année, puis on rejoint las bases

calcul_periode_retour_circ_2019 <- function(start_year, end_year) {
  swi_historique %>%
    filter(annee %in% start_year:end_year) %>%
    group_by(maille_safran, mois) %>%
    mutate(
      secheresse_circ_2019 = ifelse(annee >= 2018 & SWI_mens3 %in% tail(sort(SWI_mens3, decreasing = TRUE), 2), 1, 0),
      rang_circ_2019 = ifelse(annee >= 2018, base::rank(SWI_mens3), NA)
    ) %>%
    filter(annee == end_year) %>%
    ungroup()
}

years <- 2018:2022

secheresse_historique_par_mois <- lapply(years, function(year) {
  calcul_periode_retour_circ_2019(year - 49, year)
})

critere_meteo_2019_par_maille_et_mois <- bind_rows(secheresse_historique_par_mois)

saveRDS(critere_meteo_2019_par_maille_et_mois, paste0(path_input, 'critere_meteo_2019_par_maille_et_mois.RDS'))


### 1.2.2. Calcul des sécheresses et des rangs (méthode de 2024) 
## Désormais, nous ne prenons que 30 ans de recul. Une sécheresse est anormale si elle est dans les 
## 3 plus intenses sur 30 ans (période de retour de 10 ans). On considère également les sécheresses 
## répétées, pour lesquelles il faut une période de retour de 5 ans (rang 1 à 6) dans l'année n et 
## pour 2 années parmi les les années n-4 à n-1

# NOTE : je n'ai finalement pas utilisé le critère 2024, mais je laisse ici sa réplication

swi_historique <- arrow::read_parquet(paste0(path_input, "SWI_uniforme/swi_historique"))

## Transformation des données en valeurs mensuelles ####
swi_historique_demoyenne <- swi_historique %>%  
  mutate(
    date_complete = as.Date(paste0(annee, "-", mois, "-01")), 
    SWI_mens3 = as.numeric(gsub(",", ".", SWI_mens3)) #changer les virgules, puis transformer en numérique
  ) %>% 
  group_by(maille_safran) %>% 
  dplyr::arrange(date_complete, .by_group = TRUE) %>% 
  mutate(
    SWI_mens = {
      # Initaliser avec des NAs
      SWI_mens <- rep(NA_real_, n())
      # 2 hypothèses = approximations sur les 2 premiers mois, qu'on fixe
      SWI_mens[1] <- SWI_mens3[1]
      SWI_mens[2] <- 2 * SWI_mens3[2] - SWI_mens3[1]
      # Le reste, on démoyenne proprement
      if (n() > 2) {
        for (i in 3:n()) {
          SWI_mens[i] <- 3 * SWI_mens3[i] - SWI_mens[i - 1] - SWI_mens[i - 2]
        }
      }
      SWI_mens
    }
  )
arrow::write_parquet(swi_historique_demoyenne, paste0(path_input, "SWI_uniforme/swi_historique_demoyenne"))
# Des vérifications rapides (calcul refait à la main pour certaines années) suggèrent que c'est bon 
# Revérifier cependant si ce travail est repris ! 

## Conservons uniquement une valeur mensuelle par an 
swi_historique_demoyenne <- arrow::read_parquet(paste0(path_input, "SWI_uniforme/swi_historique_demoyenne"))

swi_historique_demoyenne_reduit <- swi_historique_demoyenne %>% 
  group_by(maille_safran, annee) %>%
  reframe(
    SWI_mens_min= min(SWI_mens),
    mois = mois[base::which.min(SWI_mens)],
    date_complete = date_complete[base::which.min(SWI_mens)],
    x= unique(x),
    y= unique(y)
  )

## Appliquons le critère 2024 sur les données annuelles obtenues
calcul_periode_retour_circ_2024 <- function(start_year, end_year) {
  swi_historique_demoyenne_reduit %>%
    mutate(annee = as.numeric(annee)) %>%
    filter(annee %in% start_year:end_year) %>%
    group_by(maille_safran) %>%
    mutate(
      RP_10 = ifelse(annee >= 2018 & SWI_mens_min %in% utils::tail(base::sort(SWI_mens_min, decreasing = TRUE), 3), 1, 0),
      RP_5 = ifelse(annee >= 2014 & SWI_mens_min %in% utils::tail(base::sort(SWI_mens_min, decreasing = TRUE), 6), 1, 0) #on veut l'année n-4 pour 2018
    ) %>%
    arrange(maille_safran, annee) %>%  # Ensure the data is sorted correctly
    mutate(
      RP_5_lag1 = dplyr::lag(RP_5, 1, default = 0),
      RP_5_lag2 = dplyr::lag(RP_5, 2, default = 0),
      RP_5_lag3 = dplyr::lag(RP_5, 3, default = 0),
      RP_5_lag4 = dplyr::lag(RP_5, 4, default = 0)
    ) %>%
    mutate(
      secheresse_repetee = purrr::map2_lgl(RP_5,
                                           purrr::pmap_dbl(list(RP_5_lag1, RP_5_lag2, RP_5_lag3, RP_5_lag4),
                                                           ~sum(c(...)) >= 2),
                                           ~.x == 1 & .y)
    ) %>%
    ungroup() %>%
    filter(annee == end_year)
}

# 30 year period for our years of interest
years <- 2018:2022
secheresse_historique_par_mois <- lapply(years, function(year) {
  calcul_periode_retour_circ_2024(year - 29, year)
})

critere_meteo_2024_par_maille_et_mois <- bind_rows(secheresse_historique_par_mois)

saveRDS(critere_meteo_2024_par_maille_et_mois, paste0(path_input, 'critere_meteo_2024_par_maille_et_mois.RDS'))

### Comptons les sécheresses obtenues
critere_meteo_2024_par_maille_et_mois %>% 
  group_by(annee) %>% 
  summarize(somme_RP10 = sum(RP_10),
            somme_pasRP10 = sum(as.numeric(RP_10==0)),
            somme_répétée = sum(secheresse_repetee))
# Même commentaire : vérifier en cas de reprise, mais ça m'a l'air ok

####  1.3. Création d'un df unique des sécheresses #### 
critere_meteo_2019_par_maille_et_mois <- readRDS(paste0(path_input, 'critere_meteo_2019_par_maille_et_mois.RDS'))
critere_meteo_2024_par_maille_et_mois <- readRDS(paste0(path_input, 'critere_meteo_2024_par_maille_et_mois.RDS'))

### 1.3.1 Calcul trimestriel en 2019
map_to_saison_2019 <- function(annee, mois) {
  saison_date <- case_when(
    mois %in% c("01", "02", "03") ~ paste0(annee, "-01-01"),
    mois %in% c("04", "05", "06") ~ paste0(annee, "-04-01"),
    mois %in% c("07", "08", "09") ~ paste0(annee, "-07-01"),
    mois %in% c("10", "11", "12") ~ paste0(annee, "-10-01")
  )
  return(as.Date(saison_date))
}

critere_meteo_2019_par_maille_et_saison <- critere_meteo_2019_par_maille_et_mois %>% 
  mutate(saison = map_to_saison_2019(annee, mois)) %>% 
  group_by(maille_safran, x, y, annee, saison) %>% 
  reframe(
    secheresse_circ_2019 = ifelse(sum(secheresse_circ_2019)>=1,1,0),
    rang_circ_2019 = min(rang_circ_2019),
    SWI_min_trimestre = min(SWI_mens3)
  ) %>% 
  mutate(annee = as.numeric(annee))

### 1.3.2 Calcul annuel en 2024 : duplication des valeurs annuelles à des fins de comparaison
map_to_saison_2024 <- function(annee) {
  saison_date <- as.Date(c(
    paste0(annee, "-01-01"),
    paste0(annee, "-04-01"),
    paste0(annee, "-07-01"),
    paste0(annee, "-10-01")
  ))
  return(saison_date)
}

critere_meteo_2024_par_maille_et_saison <- critere_meteo_2024_par_maille_et_mois %>% 
  select(-c('mois', 'date_complete', starts_with('RP_5_lag'))) %>% 
  group_by(maille_safran, annee) %>%
  # On recrée artificiellement les 4 saisons (qui partagent les mêmes valeurs)
  slice(rep(1:n(), each = 4)) %>%
  mutate(saison = rep(map_to_saison_2024(annee[1]), each = n()/4)) %>%
  ungroup() %>% 
  group_by(maille_safran, x, y, annee) %>% 
  reframe(
    secheresse_circ_2024 = ifelse(
      sum(as.numeric(secheresse_repetee==TRUE))>=1 | sum(RP_10)>=1,
      1,0),
    secheresse_repetee_circ_2024 = ifelse(
      sum(as.numeric(secheresse_repetee==TRUE))>=1,
      1,0),
    SWI_min_annuel = SWI_mens_min, 
    saison = saison
  ) 

### 1.3.3. Dataframe unique
df_critere_meteo <- critere_meteo_2019_par_maille_et_saison %>% 
  left_join(critere_meteo_2024_par_maille_et_saison, 
            by = c("maille_safran", "x", "y", "annee", "saison")) 

saveRDS(df_critere_meteo, paste0(path_input, 'critere_meteo.RDS'))

#### 1.4 Evaluation des résultats obtenus #### 
diagnostic_correspondence <- df_critere_meteo %>% 
  group_by('maille_safran', 'saison') %>% 
  mutate(true_positive = ifelse(secheresse_circ_2019==1 & secheresse_circ_2024==1,1,0),
         false_positive = ifelse(secheresse_circ_2019==1 & secheresse_circ_2024==0,1,0),
         true_negative = ifelse(secheresse_circ_2019==0 & secheresse_circ_2024==0,1,0),
         false_negative = ifelse(secheresse_circ_2019==0 & secheresse_circ_2024==1,1,0)) %>% 
  ungroup() %>% 
  reframe(
    nombre_mailles = length(unique(maille_safran)), 
    nombre_saisons = length(unique(saison)), 
    nombre_total = nombre_mailles*nombre_saisons,
    true_positives = sum(true_positive),
    false_positives = sum(false_positive),
    true_negatives = sum(true_negative),
    false_negatives = sum(false_negative)
  )

share_of_agreement = (diagnostic_correspondence$true_positives + diagnostic_correspondence$true_negatives)/diagnostic_correspondence$nombre_total*100
# 70.75% 
share_of_disagreement = (diagnostic_correspondence$false_positives + diagnostic_correspondence$false_negatives)/diagnostic_correspondence$nombre_total*100
# 29.25%, ok 
share_of_RP25 = sum(df_critere_meteo$secheresse_circ_2019)/diagnostic_correspondence$nombre_total*100
# 13,79% (en circ 2019)
share_of_rank_3 = sum(as.numeric(df_critere_meteo$rang_circ_2019 ==3))/diagnostic_correspondence$nombre_total*100
# 5,16%
share_of_rank_3_or_4 = sum(as.numeric(df_critere_meteo$rang_circ_2019 %in% c(3:4)))/diagnostic_correspondence$nombre_total*100
# 9.52%

## Un écart important entre les RP 10 à 25 (exclus) selon la définition 2019, et la définition
## 2024, qui ajoute a) les sécheresses répétées & b) l'annualisation. 
share_of_10_to_25 = diagnostic_correspondence$false_negatives/diagnostic_correspondence$nombre_total*100
# 25% 
share_of_10_to_25 = sum(as.numeric(df_critere_meteo$secheresse_circ_2019 == 0 & df_critere_meteo$rang_circ_2019 %in% c(3:5)))/diagnostic_correspondence$nombre_total*100
# 13.55%

## Ce qui nous embête : les faux positifs, c'est-à-dire les mailles non reconnues en 2024
## qui l'avaient été en 2019. Il faudra voir comment cela se couple avec les demandes CatNat
stricter_now = (diagnostic_correspondence$false_negatives)/diagnostic_correspondence$nombre_total*100
# 25.20%
less_strict_now = (diagnostic_correspondence$false_positives)/diagnostic_correspondence$nombre_total*100
# 3.93%

#### 1.5  Calcul des critières météorologiques (2019 étendu, 2024) par commune ####

### 1.5.1 Carte commune-maille
# On charge les mailles Safran : en données historiques, nous avons 8981 mailles (contre 8600 déjà en gpkg)
mailles_Safran <- arrow::read_parquet(paste0(path_input, "SWI_uniforme/swi_historique")) %>% 
  select(-c('annee', 'mois', 'SWI_mens3')) %>% 
  distinct() %>% 
  sf::st_as_sf(coords = c('x', 'y'), crs = st_crs(2154)) %>% #selon la discussion sur data.gouv, il faut l'importer en 2154
  btb::btb_ptsToGrid(names_centro = c('x', 'y'),
                     iCellSize=8000,
                     sEPSG = "2154")
plot(mailles_Safran$geometry)
sf::write_sf(mailles_Safran, paste0(path_input, "mailles_Safran.gpkg"), overwrite = TRUE)

# On charge les communes
## Note : si on reprend, il vaudrait p-e mieux apparier année par année (fusion de communes, etc.)
communes <- sf::st_read(paste0(path_input, "commune_francemetro_2023.gpkg")) %>% 
  select(surf, code, libelle, dep, reg, the_geom) 
sf::st_crs(communes) #Sélection en France métro, donc EPSG:2154

# On localise les communes
communes_avec_mailles <- sf::st_intersection(
  communes,
  mailles_Safran
)

exclus = communes %>% 
  select(code, libelle) %>% 
  anti_join(sf::st_intersection(communes, mailles_Safran) %>%
              select(code) %>% distinct(code),
            by = "code")

## Doing so, we lose Ile de Sein and Ouessant (Islands in Britanny, respectively in the 
## the Atlantic ocean and the English Channel), i.e. 1000 people. Decent.  
subset_mailles <- mailles_Safran %>% 
  #filter(as.double(maille_safran) %in% c(11208,11209,11065, 11066)) (numérotation Drias)
  filter(as.double(maille_safran) %in% c(4390, 4391, 4506, 4507))
subset_communes <- communes %>% 
  filter(code %in% c(37132, #Loches
                     37053, #Chanceaux-près-Loches
                     37049, #Chambourg-sur-Indre
                     37162, #Mouzay
                     37265, #Varennes
                     37238, #Saint-Senoch
                     37183, #Perrusson
                     37020, #Beaulieu-lès-Loches
                     37097, #Dolus le sec
                     37108, #Ferrière-sur-Beaulieu
                     37222 #Saint-Jean-Saint-Germain
  ))%>% 
  sf::st_transform(crs=2154)

mailles_union <- sf::st_union(subset_mailles)
subset_communes_cropped <- st_intersection(subset_communes, mailles_union)

plot_Loches <- ggplot() +
  geom_sf(data = subset_communes_cropped, fill = "#FFD485", color = "black") +
  geom_sf(data = subset_mailles, fill = "transparent", color = "black") +
  theme_void() +
  labs(caption = "  Source: Safran grids (Météo France, 2021), replication of the figure from the\n  circular n°INTE1911312C (May 2019)") +
  theme(plot.title = element_text(hjust = 0.5),
        plot.subtitle = element_text(hjust = 0.5),
        plot.caption = element_text(hjust = 0),
        axis.text = element_blank(),
        axis.title = element_blank(),
        legend.position = "none") +
  geom_sf_text(data = subset_communes_cropped, aes(label = libelle), size = 2, hjust = 0.5) +
  geom_sf_text(data = subset_mailles, aes(label = maille_safran), size = 4, hjust = 0.2, position = "identity")

print(plot_Loches)
ggsave(plot = plot_Loches, filename = paste0(path_outputs, "plot_Loches.pdf"), 
       width= 4.5, height = 5.7, dpi = 400)

### 1.5.2 Clef commune-maille
clef_appariement <- communes_avec_mailles %>%
  sf::st_drop_geometry() %>%
  select(-c('x', 'y')) %>%
  group_by(surf, code, libelle, dep, reg) %>%
  reframe(
    liste_mailles_safran = paste(unique(maille_safran), collapse = "-"), 
    nbr_intersections = length(unique(maille_safran))
  )
plot(clef_appariement$nbr_intersections)
saveRDS(clef_appariement, paste0(path_input, 'clef_appariement.RDS'))
# 34814 communes, i.e. toutes sauf les 2 îles bretonnes

### 1.5.3 Df commune-mailles
communes_avec_mailles <- communes_avec_mailles %>%
  sf::st_drop_geometry() %>%
  select(-c('x', 'y')) %>%
  group_by(surf, code, libelle, dep, reg) %>%
  mutate(
    liste_mailles_safran = paste(unique(maille_safran), collapse = "-"), 
    nbr_intersections = length(unique(maille_safran))
  )
saveRDS(communes_avec_mailles, paste0(path_input, 'communes_avec_mailles.RDS'))

### 1.5.4. Création d'une table unique
# Notre objectif est d'avoir une colonne code commune, une colonne saison (date), une colonne sécheresse anormale, une colonne rang, etc.
# Pour ce faire : il me faut prendre le rang le plus petit de l'ensemble des mailles intersectées. 
# On veut 34814 communes x 5 années x 4 saisons = 696 280 observations

# On charge les bases
communes_avec_mailles <- readRDS(paste0(path_input, 'communes_avec_mailles.RDS'))

df_critere_meteo <- readRDS(paste0(path_input, 'critere_meteo.RDS'))
saisons <- unique(df_critere_meteo$saison)
saisons_df <- tibble(saison = saisons)

communes_avec_critere_meteo_long <- communes_avec_mailles %>%
  # D'abord, on étend afin d'avoir, pour chaque commune x maille, les 20 saisons
  mutate(dummy = 1) %>%
  full_join(saisons_df %>% mutate(dummy = 1), by = "dummy") %>%
  select(-dummy) %>% 
  # Pour l'instant, ça donne 87453*20 = 1,7M d'observations. On y ajoute les valeurs pour chaque
  # maille x saison les valeurs des critères météo 
  left_join(df_critere_meteo %>% select(-c('x', 'y')), 
            by = c('maille_safran', 'saison'))

communes_avec_critere_meteo <- communes_avec_critere_meteo_long %>% 
  group_by(saison, code, surf, libelle, dep, reg, liste_mailles_safran, nbr_intersections, annee) %>% 
  # On sélectionne la maille la plus avantageuse
  reframe(
    secheresse_circ_2019 = max(secheresse_circ_2019),
    rang_circ_2019 = min(rang_circ_2019),
    SWI_min_trimestre = min(SWI_min_trimestre),
    secheresse_circ_2024 = max(secheresse_circ_2024), 
    SWI_min_annuel = min(SWI_min_annuel), 
    secheresse_repetee_circ_2024 = max(secheresse_repetee_circ_2024)
  )

saveRDS(communes_avec_critere_meteo, paste0(path_input, 'communes_avec_critere_meteo.RDS'))

# Bilan : on a bien 34814 communes x 5 années x 4 saisons = 696 280 observations

###############################################################
###   2.  Travail sur les reconnaissances CatNat observées  ###
###############################################################

#### 2.1 Statistiques descriptives sur les catastrophes historiques ####

### 2.1.1. Chargement des bases
reco_catnat <- read_delim(paste0(path_input, "bd_catnat_07_2024/BDarretes_complete.csv"), 
                          delim = ";",
                          lazy = TRUE,
                          locale = locale(encoding = "UTF-8"))%>%
  dplyr::mutate_at(vars(dat_deb, dat_fin, dat_pub_arrete, dat_pub_jo),
                   funs(as.Date(., format = "%d/%m/%Y"))) %>% 
  dplyr::filter(lubridate::year(dat_pub_arrete) >= 2018,#les événements, en revanche, datent d'avant 
                num_risque_jo == "18") %>% #vérifier à terme que c bien ça dans le JO
  dplyr::rename(decision = Décision) %>% 
  dplyr::mutate(duration_RGA = dat_fin-dat_deb, 
                duree_reco_deb = dat_pub_arrete - dat_deb, 
                duree_reco_fin = dat_pub_arrete - dat_fin)

### 2.1.2. Durée des événements
stats_desc <- reco_catnat %>% 
  summarize(duree_med_reco_deb = median(dat_pub_arrete - dat_deb),
            duree_moy_reco_deb = mean(dat_pub_arrete - dat_deb),
            duree_min_reco_deb = min(dat_pub_arrete - dat_deb),
            duree_max_reco_deb = max(dat_pub_arrete - dat_deb),
            duree_med_reco_fin = median(dat_pub_arrete - dat_fin),
            duree_moy_reco_fin = mean(dat_pub_arrete - dat_fin), 
            duree_min_reco_fin = min(dat_pub_arrete - dat_fin),
            duree_max_reco_fin = max(dat_pub_arrete - dat_fin))

# Graphique des durées d'attente: 
graph_cumsum_df <- reco_catnat %>%
  dplyr::arrange(duree_reco_deb) %>%
  dplyr::mutate(cumulatif_debut = seq_along(duree_reco_deb) / n()) %>%
  dplyr::arrange(duree_reco_fin) %>%
  dplyr::mutate(cumulatif_fin = seq_along(duree_reco_fin) / n())

midnight_blue <- "#191970"
raw_sienna <- "#D68A59"
a_deb <- as.numeric(graph_cumsum_df %>% filter(cumulatif_debut <= 0.95) %>% summarise(max_deb = max(duree_reco_deb)) %>% pull(max_deb))
a_fin <- as.numeric(graph_cumsum_df %>% filter(cumulatif_fin <= 0.95) %>% summarise(max_fin = max(duree_reco_fin)) %>% pull(max_fin))

plot_span <- ggplot(graph_cumsum_df) +
  geom_line(aes(x = as.numeric(duree_reco_deb), y = as.numeric(cumulatif_debut)), color = midnight_blue, linewidth = 1.2) +
  geom_line(aes(x = as.numeric(duree_reco_fin), y = as.numeric(cumulatif_fin)), color = raw_sienna, linewidth = 1.2) +
  labs(
    title = "Number of days between the **<span style='color:#191970;'>start</span>** or the **<span style='color:#D68A59;'>end</span>** of the event and the interministerial order ruling on the recognition of the natural disaster",
    x = "Duration (days)",
    y = "Cumulative share of events"
  ) + 
  scale_x_continuous(
    breaks = c(seq(0, a_deb, by = 30), max(graph_cumsum_df$duree_reco_deb)),
    labels = c(seq(0, a_deb, by = 30), max(graph_cumsum_df$duree_reco_deb)),
    limits = c(0, max(graph_cumsum_df$duree_reco_deb)) # Ensure the axis starts at 0
  ) +
  ggbreak::scale_x_break(c(a_deb, as.numeric(max(graph_cumsum_df$duree_reco_deb)) - 30)) +
  scale_y_continuous(labels = scales::percent_format(), limits = c(0, 1)) +
  theme_minimal() +
  theme(
    plot.title = ggtext::element_markdown(size = 14, face = "bold"),  # Apply markdown styling to the title
    plot.caption = element_text(hjust = 0)  # Left-align the note
  ) + 
  labs(caption = "Note: Around 25% of municipalities receive an answer regarding their claim for natural disaster recognition a year after the beginning of the alleged clay swelling event.\nSource: BD Arrêtés CatNat, 2018-2024")

ggsave(paste0(path_outputs, "span_recognition.pdf"), device ='pdf',
       plot = plot_span, width = 13, height = 4.2)

#### 2.2 Ajout des données CatNat au dataframe climatique obtenu : harmonisation, vérification ####
## 2.2.1 Chargement des bases
communes_avec_critere_meteo <- readRDS(paste0(path_input, 'communes_avec_critere_meteo.RDS'))

reco_catnat <- read_delim(paste0(path_input, "bd_catnat_07_2024/BDarretes_complete.csv"), 
                          delim = ";",
                          lazy = TRUE,
                          locale = locale(encoding = "UTF-8"))%>%
  dplyr::mutate_at(vars(dat_deb, dat_fin, dat_pub_arrete, dat_pub_jo),
                   funs(as.Date(., format = "%d/%m/%Y"))) %>% 
  dplyr::filter(lubridate::year(dat_pub_arrete) >= 2018,#les événements, en revanche, datent d'avant 
                num_risque_jo == "18") %>% #vérifier à terme que c bien ça dans le JO
  dplyr::rename(decision = Décision) %>% 
  dplyr::mutate(duration = dat_fin-dat_deb)

# Qu'avons-nous ? 
max(reco_catnat$duration) #365 jours
min(reco_catnat$duration) #0, but all refused -> on va considérer le trimestre

## 2.2.2. Nature de la décision par saison
extend_by_season <- function(df) {
  extended_df <- df %>% 
    mutate(dat_deb_standard = case_when(
      month(dat_deb) < 4 ~ as.Date(paste0(year(dat_deb), "-01-01")),
      month(dat_deb) < 7 ~ as.Date(paste0(year(dat_deb), "-04-01")),
      month(dat_deb) < 10 ~ as.Date(paste0(year(dat_deb), "-07-01")),
      TRUE ~ as.Date(paste0(year(dat_deb), "-10-01"))
    )) %>% 
    rowwise() %>%
    do({
      row <- as_tibble(.)
      if (row$duration < 100) {
        row %>% mutate(saison = dat_deb_standard)
      } else if (row$duration < 200) {
        bind_rows(
          row %>% mutate(saison = dat_deb_standard),
          row %>% mutate(saison = dat_deb_standard %m+% months(3))
        )
      } else if (row$duration < 300) {
        bind_rows(
          row %>% mutate(saison = dat_deb_standard),
          row %>% mutate(saison = dat_deb_standard %m+% months(3)),
          row %>% mutate(saison = dat_deb_standard %m+% months(6))
        )
      } else {
        bind_rows(
          row %>% mutate(saison = dat_deb_standard),
          row %>% mutate(saison = dat_deb_standard %m+% months(3)),
          row %>% mutate(saison = dat_deb_standard %m+% months(6)),
          row %>% mutate(saison = dat_deb_standard %m+% months(9))
        )
      }
    }) %>%
    ungroup() 
  extended_df
}

# Mes vérifications suggèrent que ça fonctionne : à reprendre si souhaité
reco_catnat_datee <- extend_by_season(reco_catnat)

reco_catnat_datee_clean <- reco_catnat_datee %>% 
  select(-c('dept','lib_commune','lat.', 'long.', 'num_risque_jo', 'lib_risque_jo', 
            'duration', 'dat_deb', 'dat_fin', 'dat_deb_standard' ))

## 2.2.3. Df final
df_climat <- communes_avec_critere_meteo %>% 
  left_join(reco_catnat_datee_clean,by = c('saison'='saison',
                                           'code'='cod_commune')) %>% 
  mutate(demande = ifelse(!is.na(decision), 1, 0), 
         reconnaissance = case_when(
           decision == 'Reconnue' | decision == 'Reconnue (2ème Rec)'~ 1,
           decision == 'Non reconnue' ~ 0, 
           TRUE ~ NA))

# Parfois, il y a 2 demandes pour la même période (par exemple, une demande ratée, puis une autre réussie, sans
# doute pour des vices de procédure). On conserve, par ordre de priorité : les reconnaissances, puis les arrêtés
# les plus récents en cas de refus répété. 

df_climat_nettoyee <- df_climat %>% 
  group_by(saison, code) %>% 
  arrange(desc(decision == 'Reconnue'), desc(decision == 'Reconnue (2ème Rec)'), desc(dat_pub_arrete)) %>% 
  slice(1) %>% 
  ungroup()

# On a désormais bien 20*34814=696280 lignes. On y ajoute le nombre de reconnaissance jusqu'en t:

df_climat_nettoyee <- df_climat_nettoyee %>% 
  group_by(code) %>% 
  arrange(saison) %>% 
  mutate(reconnaissance = replace_na(reconnaissance, 0), # Replace NA with 0
         reco_catnat_up_to_t = cumsum(reconnaissance == 1),
         demande_catnat_up_to_t = cumsum(demande == 1)) %>%
  ungroup()


saveRDS(df_climat_nettoyee, paste0(path_input,'df_climat.RDS'))

# évaluons nos pertes : presqu'aucune
sum(!is.na(df_climat$decision)) #68094
reco_catnat_datee %>% filter(year(saison)>=2018 & year(saison) < 2023) %>% nrow() #68107

# a) Comptons les reconnaissances réelles que nous ne retrouvons pas : 
unmatched_rows <- reco_catnat_datee_clean %>%
  filter(year(saison)>=2018 & year(saison) <= 2022) %>% 
  anti_join(communes_avec_critere_meteo, by = c('saison' = 'saison', 'cod_commune'='code'))
unmatched_reco <- unmatched_rows %>% filter(decision=='Reconnue') #7 observations
unmatched_refus <- unmatched_rows %>% filter(decision!='Reconnue') #6 observations

# Il y a seulement 4 communes que nous n'avons pas dans notre base avec le critère répliqué, pour 
# 7 saisons d'acceptation et 6 de refus. Il nous faut donc retrouver les communes 25628,26219,
# 71492 et 02077. motif = fusions de communes (1er janvier 2022 ou 2023 ). Prendre la géo
# des communes de 2021 ! Aussi, faire un floor sur le rang ! 

# b) Comptons les reconnaissances prédites que nous ne trouvons pas
unmatched_rows <- communes_avec_critere_meteo %>%
  anti_join(reco_catnat_datee_clean, by = c('saison' = 'saison', 'code' = 'cod_commune'))
unmatched_reco <- unmatched_rows %>% filter(secheresse_circ_2019==1) #près de 100K ?? lol

# Près de 100K saisons où une commune aurait pu demander, obtenir, et ne l'a pas fait. Crédible ? 
# Difficile à dire : notons que toutes les communes ne sont pas sur des plaques argileuses !

length(unique(unmatched_reco$code)) #correspond à 27026 communes ! 

# c) Comptons les reconnaissances que nous avons modélisé comme refus
df_climat %>% filter(decision=='Reconnue' & secheresse_circ_2019 == 0) %>% nrow #410 échecs
df_climat %>% filter(decision=='Reconnue' & secheresse_circ_2019 == 1) %>% nrow #25563 réussites

# d) Comptons les refus que nous avons modélisé comme reconnaissance
df_climat %>% filter(decision!='Reconnue' & secheresse_circ_2019 == 1) %>% nrow #584 échecs
df_climat %>% filter(decision!='Reconnue' & secheresse_circ_2019 == 0) %>% nrow #41537 réussites

# Je trouve mon taux de réussite décent. ngl, bravo sacha. Notons, par ailleurs, que des refus
# peuvent s'expliquer par des erreurs de procédure de la part des maires (eg : demande sur 2 
# années civiles, dates imprécises) ! La première catégorie, en revanche, est plus problématique ... 

#############################################
###   3.  Travail sur la base consolidée  ###
#############################################

# 3.1 Chargement de la base
df_climat <- readRDS(paste0(path_input, 'df_climat.RDS'))

# 3.2 Tableau : part des demandes par saison (total), et parmi ceux qui l'auraient validé
# Part des demandes
total <- nrow(df_climat)
catnat_claims <- sum(df_climat$demande == 1, na.rm = TRUE)
share_catnat_claims <- catnat_claims / total * 100 #9.75%

# Share of CatNat claims among municipalities which could have validated the meteorological criterion
total_meteo_criterion <- nrow(df_climat %>% filter(secheresse_circ_2019 == 1))
catnat_claims_meteo_criterion <- nrow(df_climat %>% filter(secheresse_circ_2019 == 1  & demande == 1))
share_catnat_meteo_criterion <- catnat_claims_meteo_criterion / total_meteo_criterion * 100 # 20%...

# 3.3 Graphique : part des demandes par rang, part des reconnaissance par rang (part totale, et subset)
df_rank <- df_climat %>% 
  mutate(rang = floor(rang_circ_2019)) %>% 
  group_by(rang) %>% 
  reframe(
    share_claims = sum(as.numeric(demande==1), na.rm=TRUE)/n(),
    share_reco = sum(as.numeric(reconnaissance==1), na.rm=TRUE)/n(),
    share_reco_among_claims = sum(as.numeric(reconnaissance==1), na.rm=TRUE)/sum(as.numeric(demande==1), na.rm=TRUE),
  )

df_rank_cumul <- df_climat %>%
  mutate(rang = floor(rang_circ_2019)) %>% 
  group_by(rang) %>% 
  reframe(
    transactions = n(),
    nbr_claims = sum(as.numeric(demande==1), na.rm=TRUE),
    nbr_reco = sum(as.numeric(reconnaissance==1), na.rm=TRUE)
  ) %>% 
  ungroup() %>% 
  mutate(
    cumulative_nbr_transactions = cumsum(transactions),
    cumulative_nbr_claims = cumsum(nbr_claims),
    sum_transactions = sum(transactions, na.rm = TRUE),
    sum_claims = sum(nbr_claims, na.rm = TRUE),
    share_claims = cumulative_nbr_claims/sum_claims,
    share_transactions = cumulative_nbr_transactions/sum_transactions
  )

# Plot
ggplot(df_rank_cumul, aes(x = rang, y = share_claims)) +
  geom_line() +
  labs(
    title = "Cumulative Share of Share Claims by Rang",
    x = "Rang",
    y = "Cumulative Share of Share Claims"
  ) +
  theme_minimal() + 
  geom_vline(xintercept = 2, color = "black", linetype = "dotted", size = 0.5) +
  geom_vline(xintercept = 4, color = "black", linetype = "dotted", size = 0.5)

# Faisons de la soupe de représentation graphique : 
left_data <- df_rank %>% filter(rang < 2)
right_data <- df_rank %>% filter(rang >= 2)

# Models for 'share_claims'
model_left_claims <- lm(share_reco_among_claims ~ rang, data = left_data)
model_right_claims <- lm(share_reco_among_claims ~ rang, data = right_data)

# Create the plot
midnight_blue <- "#191970"
raw_sienna <- "#D68A59"

plot_rank <- ggplot(df_rank, aes(x = rang)) +
  geom_point(aes(y = share_reco_among_claims), color = midnight_blue, alpha = 0.7) +
  labs(title = "Share of CatNat claims accepted by Soil Wetness Index (SWI) rank",
       x = "SWI Rank",
       y = NULL) +
  theme_minimal() +
  geom_vline(xintercept = 2.5, color = raw_sienna, linetype = "dotted", size = 0.8) +
  scale_y_continuous(labels = scales::percent_format(), limits = c(0, 1)) +
  theme(
    plot.title = ggtext::element_markdown(size = 14, face = "bold"),  # Apply markdown styling to the title
    plot.caption = element_text(hjust = 0, size = 11)  # Left-align the note
  ) + 
  labs(caption = "Note: The SWI assessed is the lowest across all Safran grids intersecting with municipality area for the period over which recognition\nof the state of natural disaster is requested. Rank 1 and 2 correspond to a return period over 25, and should be granted recognition.\nData: SWI, Météo France; BD Catnat. Replication made by the author.")

ggsave(paste0(path_outputs, "Share_reco_per_rank.pdf"), plot = plot_rank, width = 9.7, height = 3.65)

plot_rank_claims <- ggplot(df_rank_cumul, aes(x = rang)) +
  geom_line(aes(y = share_claims, color = "Share of Claims"), size = 1) +
  geom_line(aes(y = share_transactions, color = "Share of Transactions"), size = 1) +
  scale_color_manual(values = c("Share of Claims" = midnight_blue, 
                                "Share of Transactions" = raw_sienna), 
                     breaks = c("Share of Claims", "Share of Transactions"), 
                     name = NULL) +
  labs(title = "Share of transactions and CatNat claims by Soil Wetness Index (SWI) rank",
       x = "SWI Rank",
       y = NULL) +
  theme_minimal() +
  geom_vline(xintercept = 2, color = 'black', linetype = "dotted", size = 0.8) +
  scale_y_continuous(labels = scales::percent_format(), limits = c(0, 1)) +
  theme(
    plot.title = ggtext::element_markdown(size = 14, face = "bold"),  # Apply markdown styling to the title
    plot.caption = element_text(hjust = 0, size = 11),  # Left-align the note
    legend.position = 'top'
  ) #+   labs(caption = "Note: The SWI assessed is the lowest across all Safran grids intersecting with municipality area for the period over which recognition\nof the state of natural disaster is requested. Rank 1 and 2 correspond to a return period over 25, and should be granted recognition.\nData: SWI, Météo France; BD Catnat. Replication made by the author.")
ggsave(paste0(path_outputs, "Share_claims_per_rank.pdf"), plot = plot_rank_claims, width = 9.7, height = 3.65)


# Par curiosité, regarder : 
reg1 <- lm(data = df_climat, reconnaissance ~nbr_intersections)
summary(reg1) #positif et faiblement significatif

reg2 <- lm(data = df_climat, reconnaissance ~nbr_intersections + surf)
summary(reg2) #positif et significatif

reg3 <- lm(data = df_climat, reconnaissance ~nbr_intersections + factor(saison))
summary(reg3) #positif et significatif

reg4 <- lm(data = df_climat, reconnaissance ~nbr_intersections + factor(saison) + surf)
summary(reg4) #positif et significatif, plus fort, en gros +2%

reg5 <- lm(data = df_climat, I(reconnaissance==0) ~nbr_intersections + factor(saison) + surf)
summary(reg5) 

