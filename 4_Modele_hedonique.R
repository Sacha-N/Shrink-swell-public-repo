# Manipulation de données
library(dplyr)
library(tidyr)
library(stringr)
library(arrow)
library(hutils)
library(lubridate)
library(readr)
options(arrow.use_threads = TRUE)
arrow::set_cpu_count(parallel::detectCores() %/% 4)

# Graphs, tables, maps
library(sf)
library(ggplot2)
library(knitr)
library(kableExtra)

# Metrics
library(fixest)
library(corrplot)
library(car)

# Chemin
path_input <- "Z:/Memoire/dv3f_memoire/Inputs_climat/"
path_base_intermediaire <- "Z:/Memoire/dv3f_memoire/Bases_intermediaires/"
path_outputs <- "Z:/Memoire/dv3f_memoire/Outputs/"
rep_donnees <-  "P:/PSAR_SL/SL45/Lot 3/202401/bases/comptages"

################################
#### PRESENTATION DU SCRIPT ####
################################

# L'objectif de ce script est de réaliser les inférences sur les prix 

#####################################
###### 1. CHOIX DES VARIABLES ######
#####################################
table_transactions_climat <- arrow::open_dataset(paste0(path_base_intermediaire, 'table_transactions_climat'))

##  1.1 Sélection des variables possiblement intéressantes
# Pour cette régression, on peut prendre l'ensemble des transactions (2010-2022)
df_regression <- table_transactions_climat |>
  select(
    # Données climatiques
    dat_pub_jo, demande, reconnaissance, reco_catnat_up_to_t,nbr_intersections,
    ALEA, saison,reco_catnat_up_to_t,nbr_intersections,codgeo,dep,surf,rang_circ_2019,
    dat_pub_arrete,datemut,reg,
    # Données sur les transactions, les maisons et l'IRIS
    ## Numériques : 
    Valeur_fonciere, Surface_batie, anneemut,Surface_parcelle,
    sterr, taux_mentions_sur_inscrits, part_etudiants, part_log_vac,
    part_res_sec, mediane_niv_vie, rap_inter_dec,idmutation, 
    distance_college, tx_chom_LD,part_cadres,dens_log,
    part_3039,part_couple_enf,
    ## Catégorielles : 
    Trois_pieces_et_moins, Cinq_pieces, Six_pieces, Sept_pieces_et_plus, #omise = 4 pièces
    Recent,P_1914,P1914_1944,P1945_1960,P1961_1974,P1975_1989,P2013_, #omise = P1990_2012
    Deux_niveaux_et_plus, #omise = 1 niveau
    Sans_SdB,Deux_SdB_et_plus, #omise = 1 SDB
    Sans_garage,Deux_garages_et_plus, #omise = 1 garage
    Gare_500m,Gare_3km, Gare_3km_5km, Gare_5km_7km, Gare_7km_10km, #omise = Gare_p10km
    Metro_tram_300m, Metro_tram_300m_600m, Metro_tram_600m_1k, #omise = Metro_tram_p1k_ou_non
    Sevesosh_1km, Sevesosh_1km_10km, #omise = Sevesosh_p10km
    Appartenance_QPV, QPV_500m, # omise QPV_p500m_ou_non
    PEB_A, PEB_B, PEB_C, PEB_D, #omise = pas de PEB (peut être faut il créer la variable)
    cote_300m,cote_300m_1km, #omise = cote_p1km
    ## Dummies: 
    Terrasse, Piscine, Autre_dependance,capacite_tour,
    ## Variables à la COMMUNE (donc uniquement lorsque dep FE)
    evol_nb_menage, part_log_soc, nb_log_total,
    Tourist_tres_eleve, Tourist_eleve, # omise = Tourist_faible
    ) |>
  mutate(post_ELAN = ifelse(anneemut>=2020, 1, 0),
         ALEA = ifelse(is.na(ALEA), 'Nul', ALEA),
         RGA_moyen_fort = ifelse(ALEA == 'Moyen' | ALEA == 'Fort',1,0), 
         log_valeur_fonciere = log(Valeur_fonciere),
         log_surface_batie= log(Surface_batie), 
         log_surface_parcelle= log(Surface_parcelle+1), 
         log_surface_terrain = log(sterr+1)) |> #added 1 to prevent log(0)
  collect() |>
  group_by(codgeo) %>% 
  mutate(
    dep = ifelse(is.na(dep), first(na.omit(dep)), dep),
    reg = as.factor(ifelse(is.na(reg), first(na.omit(reg)), reg)),
    nbr_intersections = ifelse(is.na(nbr_intersections), first(na.omit(nbr_intersections)), nbr_intersections)
  ) %>%
  ungroup() |>
  mutate(
    threshold = quantile(Valeur_fonciere, 0.9999),
    saison_arrete =  case_when(
      lubridate::month(dat_pub_arrete) %in% c(1, 2, 3) ~ paste0(lubridate::year(dat_pub_arrete), "-01-01"),
      lubridate::month(dat_pub_arrete) %in% c(4, 5, 6) ~ paste0(lubridate::year(dat_pub_arrete), "-04-01"),
      lubridate::month(dat_pub_arrete) %in% c(7, 8, 9) ~ paste0(lubridate::year(dat_pub_arrete), "-07-01"),
      lubridate::month(dat_pub_arrete) %in% c(10, 11, 12) ~ paste0(lubridate::year(dat_pub_arrete), "-10-01"),
      TRUE ~ NA
    )
  ) %>% 
  filter(Valeur_fonciere<=threshold)#4.7M, or 526 transactions in the top 0.01%

# Count NA values for each variable
na_counts <- sapply(df_regression, function(x) sum(is.na(x)))
na_counts_df <- data.frame(Variable = names(na_counts), NA_Count = na_counts)
na_counts_df_ordered <- na_counts_df[order(-na_counts_df$NA_Count), ]
print(na_counts_df_ordered)

## 1.2 Tests de collinéarités
### 1.2.1. Corplot for the numerical variables
midnight_blue <- "#191970"
raw_sienna <- "#D68A59"
col_palette <- colorRampPalette(c(midnight_blue, "white", raw_sienna))(200)

numerical_columns <- df_regression %>% 
  ungroup() %>% 
  select(Valeur_fonciere, Surface_batie, Surface_parcelle, 
         part_log_soc, nb_log_total,distance_college, tx_chom_LD,
         dens_log, part_3039,part_couple_enf,
         taux_mentions_sur_inscrits, part_etudiants, 
         part_log_vac, part_res_sec, mediane_niv_vie, rap_inter_dec) 

names(numerical_columns) <- c(
  "Property Value", "Built-up Area", "Plot Area", 
  'Share of social housing', 'Number of dwellings','Distance to middle-school', 'Long-term unemployment rate',
  'Density of housing', 'Share of population aged 30-39', 'Share of couples with children', 
  "Share of middle schoolers achieving honours","Share of students", 
  "Share of vacant dwellings", "Share of secondary residence", 
  "Median living standard", "Interdecile ratio"
)

# Calculate the correlation matrix with renamed labels
cor_matrix <- cor(numerical_columns, use = "pairwise.complete.obs")

# Plot the correlation matrix
png(paste0(path_outputs, "correlation_plot.png"), width = 1600, height = 1300, res = 150)
corrplot(cor_matrix, method = "color", type = "upper", 
         tl.col = "black", tl.srt = 45, 
         col = col_palette,
         addCoef.col = "black", number.cex = 0.7)
dev.off()

### 1.2.2 Complete corplot for categorical variables
variables <- c("log_valeur_fonciere",  "RGA_moyen_fort", "anneemut", 
               "log_surface_batie", "log_surface_parcelle", 
               "Trois_pieces_et_moins", "Cinq_pieces", "Six_pieces", "Sept_pieces_et_plus",
               "Recent", "P_1914", "P1914_1944", "P1945_1960", "P1961_1974", "P1975_1989", "P2013_",
               "Deux_niveaux_et_plus", "Sans_SdB", "Deux_SdB_et_plus", 
               "Sans_garage", "Deux_garages_et_plus","Terrasse", "Piscine", "Autre_dependance",
               "tx_chom_LD", "part_3039", "part_couple_enf", "part_log_vac", "dens_log",
               "part_res_sec", "rap_inter_dec", "mediane_niv_vie", "part_etudiants",
               "distance_college", "taux_mentions_sur_inscrits",
               "Gare_500m", "Gare_3km", "Gare_3km_5km", "Gare_5km_7km", "Gare_7km_10km", 
               "Metro_tram_300m", "Metro_tram_300m_600m", "Metro_tram_600m_1k",
               "Sevesosh_1km", "Sevesosh_1km_10km",
               "Appartenance_QPV", "QPV_500m",
               "PEB_A", "PEB_B", "PEB_C", "PEB_D",
               "cote_300m", "cote_300m_1km",
               "nb_log_total", "part_log_soc"
)

df_subset <- df_regression[, variables]
cor_matrix <- cor(df_subset, use = "pairwise.complete.obs")

# Search for high cor
threshold <- 0.5
high_cor_matrix <- cor_matrix
high_cor_matrix[abs(high_cor_matrix) < threshold] <- NA
diag(high_cor_matrix) <- NA

high_cor_df <- as.data.frame(as.table(high_cor_matrix))
high_cor_df <- na.omit(high_cor_df)
print(high_cor_df)

# We saw that tourists are too strongly correlated with the share of 2dary residence (removed)
# then 3 piece et moins with surface batie (but it's not too bad, it's -0.51)

### 1.2.3. VIF
## Checking one last time colinearity (without FE)
lm_model <- lm(
  log_valeur_fonciere ~ RGA_moyen_fort + factor(anneemut) +factor(dep)+
  ## Maisons
  log_surface_batie + log_surface_parcelle +
  Trois_pieces_et_moins+ Cinq_pieces+ Six_pieces+ Sept_pieces_et_plus+ #omise = 4 pièces
  Recent+P_1914+P1914_1944+P1945_1960+P1961_1974+P1975_1989+P2013_+ #omise = P1990_2012
  Deux_niveaux_et_plus+ #omise = 1 niveau
  Sans_SdB+Deux_SdB_et_plus+ #omise = 1 SDB
  Sans_garage+Deux_garages_et_plus+ #omise = 1 garage
  Terrasse+ Piscine+ Autre_dependance +
  ## Iris
  tx_chom_LD + part_3039 +part_couple_enf +part_log_vac+dens_log +
  part_res_sec+ rap_inter_dec+ mediane_niv_vie+ part_etudiants+
  ## 200m
  distance_college + taux_mentions_sur_inscrits+
  Gare_500m+Gare_3km+ Gare_3km_5km+ Gare_5km_7km+ Gare_7km_10km+ #omise = Gare_p10km
  Metro_tram_300m + Metro_tram_300m_600m + Metro_tram_600m_1k + #omise = >1km
  Sevesosh_1km+ Sevesosh_1km_10km+ #omise = Sevesosh_p10km
  Appartenance_QPV+ QPV_500m+ # omise QPV_p500m_ou_non
  PEB_A+ PEB_B+ PEB_C+ PEB_D+ #omise = pas de PEB (peut être faut il créer la variable)
  cote_300m+cote_300m_1km+ #omise = cote_p1km
  ## Commune: 
  nb_log_total + part_log_soc,
  data = df_regression)

vif_values <- car::vif(lm_model)
print(vif_values)

## So we don't have the dep values, because lm can't take it and vif cannot handle feols,
## but there are no VIF values above 2.5 : vif = 1 is no correlation, and between 1 and
## 5 = moderate, above 10 = critical. So we're good

## 1.3 Modele sans RGA
myDict <- c("(Intercept)" = "Constant",
            "RGA_moyen_fort" = 'Exposed to shrink-swell',
            "post_ELAN" = 'Post ELAN law',
            'reco_catnat_since_2003' = 'Number of past CatNat recognitions (2003)', 
            'reco_catnat_up_to_t' = 'Number of past CatNat recognitions (2018)', 
            'time_modified' = 'Years to treatment',
            'treatment' = 'Treatment (return period)',
            'utter_treatment' = 'Treatment (return period x clay)',
            "log_surface_batie" = "Built-up area (log)", 
            "log_surface_parcelle" = "Plot area (log)",
            "Trois_pieces_et_moins" = "Three rooms or less", 
            "Cinq_pieces" = "Five rooms",
            "Six_pieces" = "Six rooms",
            "Sept_pieces_et_plus" = "Seven rooms or more",
            "Recent" = "Recent construction",
            "P_1914" = "Built before 1914",
            "P1914_1944" = "Built 1914-1944",
            "P1945_1960" = "Built 1945-1960",
            "P1961_1974" = "Built 1961-1974",
            "P1975_1989" = "Built 1975-1989",
            "P2013_" = "Built 2013 and after",
            "Deux_niveaux_et_plus" = "Two levels or more",
            "Sans_SdB" = "No bathroom",
            "Deux_SdB_et_plus" = "Two bathrooms or more",
            "Sans_garage" = "No garage",
            "Deux_garages_et_plus" = "Two garages or more",
            "Terrasse" = "Terrace",
            "Piscine" = "Swimming pool",
            "Autre_dependance" = "Other dependency",
            "tx_chom_LD" = "Long-term unemployment",
            "part_3039" = "30-39 (share)",
            "part_couple_enf" = "With children (share)",
            "part_log_vac" = "Vacant housing (share)",
            "dens_log" = "Housing density",
            "part_res_sec" = "Secondary residence (share)",
            "rap_inter_dec" = "Decile ratio",
            "mediane_niv_vie" = "Median standard of living",
            "part_etudiants" = "Proportion of students",
            "distance_college" = "Distance to middle-school",
            "taux_mentions_sur_inscrits" = "Middle-schoolers with honours (share)",
            "Gare_500m" = "Station: <500m",
            "Gare_3km" = "Station: <3km",
            "Gare_3km_5km" = "Station: <5km",
            "Gare_5km_7km" = "Station: <7km",
            "Gare_7km_10km" = "Station: <10km",
            "Metro_tram_300m" = "Metro/tram: <300m",
            "Metro_tram_300m_600m" = "Metro/tram: <600m",
            "Metro_tram_600m_1k" = "Metro/tram: <1km",
            "Sevesosh_1km" = "Seveso: <1km",
            "Sevesosh_1km_10km" = "Seveso: <10km",
            "Appartenance_QPV" = "QPV",
            "QPV_500m" = "QPV: <500m",
            "PEB_A" = "Noise exp. A",
            "PEB_B" = "Noise exp. B",
            "PEB_C" = "Noise exp. C",
            "PEB_D" = "Noise exp. D",
            "cote_300m" = "Coast: <300m",
            "cote_300m_1km" = "Coast: <1km",
            "nb_log_total" = "Sum of dwellings",
            "part_log_soc" = "Social housing (share)",
            "anneemut" = "Year of transaction",
            "dep" = "Department", 
            "codgeo" = "Municipality", 
            "log_valeur_fonciere" = "House prices (log)")

fhedonic_model <- log_valeur_fonciere ~ 
  ## Maisons
  log_surface_batie + log_surface_parcelle +
  Trois_pieces_et_moins+ Cinq_pieces+ Six_pieces+ Sept_pieces_et_plus+ #omise = 4 pièces
  Recent+P_1914+P1914_1944+P1945_1960+P1961_1974+P1975_1989+P2013_+ #omise = P1990_2012
  Deux_niveaux_et_plus+ #omise = 1 niveau
  Sans_SdB+Deux_SdB_et_plus+ #omise = 1 SDB
  Sans_garage+Deux_garages_et_plus+ #omise = 1 garage
  Terrasse+ Piscine+ Autre_dependance +
  ## Iris
  tx_chom_LD + part_3039 +part_couple_enf +part_log_vac+dens_log +
  part_res_sec+ rap_inter_dec+ mediane_niv_vie+ part_etudiants+
  ## 200m
  distance_college + taux_mentions_sur_inscrits+
  Gare_500m+Gare_3km+ Gare_3km_5km+ Gare_5km_7km+ Gare_7km_10km+ #omise = Gare_p10km
  Metro_tram_300m + Metro_tram_300m_600m + Metro_tram_600m_1k + #omise = >1km
  Sevesosh_1km+ Sevesosh_1km_10km+ #omise = Sevesosh_p10km
  Appartenance_QPV+ QPV_500m+ # omise QPV_p500m_ou_non
  PEB_A+ PEB_B+ PEB_C+ PEB_D+ #omise = pas de PEB (peut être faut il créer la variable)
  cote_300m+cote_300m_1km | anneemut + codgeo #omise = cote_p1km
  # Communes :   + nb_log_total + part_log_soc 

# LaTeX output
hedonic_model <- fixest::feols(fhedonic_model, data = df_regression, cluster = ~codgeo)


etable(hedonic_model, tex = TRUE, 
       dict = myDict,title = 'Hedonic model applied to French house prices (2010-2022)',
       notes ="Source: Metropolitain France house sales (2010-2022), DV3F; enriched by Insee PSAR-SL", 
       digits = 3,
       fontsize = 'scriptsize',
       se.below = FALSE,
       coefstat = "se", 
       adjustbox = TRUE #remove for others
       )

################################
###### 2. Price-in du RGA ######
################################

## Une table unique : 2 OLS avec RGA, 1 DiD avec RGA

## 2.1. OLS : cross section RGA 
fOLS_no_controls <- log_valeur_fonciere ~  RGA_moyen_fort | anneemut
fOLS_with_controls <-  log_valeur_fonciere ~ RGA_moyen_fort +
  ## Maisons
  log_surface_batie + log_surface_parcelle +
  Trois_pieces_et_moins+ Cinq_pieces+ Six_pieces+ Sept_pieces_et_plus+ #omise = 4 pièces
  Recent+P_1914+P1914_1944+P1945_1960+P1961_1974+P1975_1989+P2013_+ #omise = P1990_2012
  Deux_niveaux_et_plus+ #omise = 1 niveau
  Sans_SdB+Deux_SdB_et_plus+ #omise = 1 SDB
  Sans_garage+Deux_garages_et_plus+ #omise = 1 garage
  Terrasse+ Piscine+ Autre_dependance +
  ## Iris
  tx_chom_LD + part_3039 +part_couple_enf +part_log_vac+dens_log +
  part_res_sec+ rap_inter_dec+ mediane_niv_vie+ part_etudiants+
  ## 200m
  distance_college + taux_mentions_sur_inscrits+
  Gare_500m+Gare_3km+ Gare_3km_5km+ Gare_5km_7km+ Gare_7km_10km+ #omise = Gare_p10km
  Metro_tram_300m + Metro_tram_300m_600m + Metro_tram_600m_1k + #omise = >1km
  Sevesosh_1km+ Sevesosh_1km_10km+ #omise = Sevesosh_p10km
  Appartenance_QPV+ QPV_500m+ # omise QPV_p500m_ou_non
  PEB_A+ PEB_B+ PEB_C+ PEB_D+ #omise = pas de PEB (peut être faut il créer la variable)
  cote_300m+cote_300m_1km | codgeo + anneemut #omise = cote_p1km
  ## Commune: + nb_log_total + part_log_soc 

OLS_no_controls <- fixest::feols(fOLS_no_controls, data = df_regression, cluster = ~codgeo)
OLS_with_controls <- fixest::feols(fOLS_with_controls, data = df_regression, cluster = ~ codgeo)

summary(OLS_no_controls)
summary(OLS_with_controls)

## 2.2. DiD : RGA
fDiD_RGA <-  log_valeur_fonciere ~ RGA_moyen_fort*post_ELAN +
  ## Maisons
  log_surface_batie + log_surface_parcelle +
  Trois_pieces_et_moins+ Cinq_pieces+ Six_pieces+ Sept_pieces_et_plus+ #omise = 4 pièces
  Recent+P_1914+P1914_1944+P1945_1960+P1961_1974+P1975_1989+P2013_+ #omise = P1990_2012
  Deux_niveaux_et_plus+ #omise = 1 niveau
  Sans_SdB+Deux_SdB_et_plus+ #omise = 1 SDB
  Sans_garage+Deux_garages_et_plus+ #omise = 1 garage
  Terrasse+ Piscine+ Autre_dependance +
  ## Iris
  tx_chom_LD + part_3039 +part_couple_enf +part_log_vac+dens_log +
  part_res_sec+ rap_inter_dec+ mediane_niv_vie+ part_etudiants+
  ## 200m
  distance_college + taux_mentions_sur_inscrits+
  Gare_500m+Gare_3km+ Gare_3km_5km+ Gare_5km_7km+ Gare_7km_10km+ #omise = Gare_p10km
  Metro_tram_300m + Metro_tram_300m_600m + Metro_tram_600m_1k + #omise = >1km
  Sevesosh_1km+ Sevesosh_1km_10km+ #omise = Sevesosh_p10km
  Appartenance_QPV+ QPV_500m+ # omise QPV_p500m_ou_non
  PEB_A+ PEB_B+ PEB_C+ PEB_D+ #omise = pas de PEB (peut être faut il créer la variable)
  cote_300m+cote_300m_1km | codgeo + anneemut #omise = cote_p1km
## Commune: + nb_log_total + part_log_soc 

fTWFE_RGA <-log_valeur_fonciere ~ RGA_moyen_fort + i(anneemut, RGA_moyen_fort, ref = 2019)+
  ## Maisons
  log_surface_batie + log_surface_parcelle +
  Trois_pieces_et_moins+ Cinq_pieces+ Six_pieces+ Sept_pieces_et_plus+ #omise = 4 pièces
  Recent+P_1914+P1914_1944+P1945_1960+P1961_1974+P1975_1989+P2013_+ #omise = P1990_2012
  Deux_niveaux_et_plus+ #omise = 1 niveau
  Sans_SdB+Deux_SdB_et_plus+ #omise = 1 SDB
  Sans_garage+Deux_garages_et_plus+ #omise = 1 garage
  Terrasse+ Piscine+ Autre_dependance +
  ## Iris
  tx_chom_LD + part_3039 +part_couple_enf +part_log_vac+dens_log +
  part_res_sec+ rap_inter_dec+ mediane_niv_vie+ part_etudiants+
  ## 200m
  distance_college + taux_mentions_sur_inscrits+
  Gare_500m+Gare_3km+ Gare_3km_5km+ Gare_5km_7km+ Gare_7km_10km+ #omise = Gare_p10km
  Metro_tram_300m + Metro_tram_300m_600m + Metro_tram_600m_1k + #omise = >1km
  Sevesosh_1km+ Sevesosh_1km_10km+ #omise = Sevesosh_p10km
  Appartenance_QPV+ QPV_500m+ # omise QPV_p500m_ou_non
  PEB_A+ PEB_B+ PEB_C+ PEB_D+ #omise = pas de PEB (peut être faut il créer la variable)
  cote_300m+cote_300m_1km  | codgeo +  anneemut #omise = cote_p1km
  ## Commune:   nb_log_total + part_log_soc 

DiD_RGA <- fixest::feols(fDiD_RGA, data = df_regression, cluster = ~codgeo)
TWFE_RGA <- fixest::feols(fTWFE_RGA, data = df_regression, cluster = ~codgeo) #19K Nas removed,

summary(DiD_RGA)
summary(TWFE_RGA)


## 2.3 Outputs and pretrends

etable(list(OLS_no_controls, OLS_with_controls, DiD_RGA), 
       tex = TRUE, 
       dict = myDict, 
       title = 'In-pricing of shrink-swell risk in French house prices (2010-2022): OLS',
       notes = "Source: Metropolitain France house sales (2010-2022), DV3F; enriched by Insee PSAR-SL; Shrink swell exposure maps (BRGM)", 
       digits = 3,
       fontsize = 'small',
       se.below = FALSE,
       coefstat = "se",
       keep = c('RGA_moyen_fort', 'Exposed to shrink-swell', 'Post ELAN law'), 
       extralines = list("^House characteristics" = c("No", "Yes","Yes"),
                         "^Neighbourhood characteristics" = c("No", "Yes","Yes"),
                         "^Local amenities" = c("No", "Yes","Yes")
       )
)

png(paste0(path_outputs, "TWFE_RGA_95CI.pdf"), width = 1000, height = 400)
fixest::iplot(TWFE_RGA,
              ci_level = 0.95,
              main = "Impact of exposure to shrink swell through time: the ELAN law discontinuity",  # No title
              xlab = "Year of transaction",
              col = raw_sienna, 
              cex = 1.5)
dev.off()

png(paste0(path_outputs, "TWFE_RGA_99CI.pdf"), width = 1000, height = 400)
fixest::iplot(TWFE_RGA,
              ci_level = 0.99,
              main = "",  # No title
              xlab = "Year of transaction",
              col = midnight_blue,
              cex = 1.5)
dev.off()

##################################################
###### 3. Price-in de CatNat (reco totales) ######
##################################################

# 3.1 Préliminaire : compte depuis le début :
reco_catnat <- read_delim(paste0(path_input, "bd_catnat_07_2024/BDarretes_complete.csv"), 
                          delim = ";",
                          lazy = TRUE,
                          locale = locale(encoding = "UTF-8"))%>%
  dplyr::mutate_at(vars(dat_deb, dat_fin, dat_pub_arrete, dat_pub_jo),
                   funs(as.Date(., format = "%d/%m/%Y"))) %>% 
  dplyr::filter(lubridate::year(dat_pub_arrete) >= 2003,#les événements, en revanche, datent d'avant 
                num_risque_jo == "18") %>% #vérifier à terme que c bien ça dans le JO
  dplyr::rename(decision = Décision, 
                codgeo = cod_commune) %>% 
  dplyr::mutate(
    saison_arrete = as.Date(case_when(
      month(dat_pub_arrete) < 4 ~ as.Date(paste0(year(dat_pub_arrete), "-01-01")),
      month(dat_pub_arrete) < 7 ~ as.Date(paste0(year(dat_pub_arrete), "-04-01")),
      month(dat_pub_arrete) < 10 ~ as.Date(paste0(year(dat_pub_arrete), "-07-01")),
      TRUE ~ as.Date(paste0(year(dat_pub_arrete), "-10-01"))))) %>% 
  group_by(saison_arrete, codgeo) %>% 
  arrange(desc(decision == 'Reconnue'), desc(decision == 'Reconnue (2ème Rec)'), desc(dat_pub_arrete)) %>% 
  slice(1) %>% 
  ungroup() %>% 
  mutate(reconnaissance_2003 = case_when(
                  decision == 'Reconnue' | decision == 'Reconnue (2ème Rec)'~ 1,
                  decision == 'Non reconnue' ~ 0, 
                  TRUE ~ NA),
    reconnaissance_2010 = case_when(
                  lubridate::year(dat_pub_arrete) >= 2010 & (decision == 'Reconnue' | decision == 'Reconnue (2ème Rec)')~ 1,
                  lubridate::year(dat_pub_arrete) >= 2010 & decision == 'Non reconnue' ~ 0, 
                  TRUE ~ NA)) %>% 
  dplyr::select(reconnaissance_2003,reconnaissance_2010, dat_pub_arrete, codgeo,saison_arrete)

unique_codgeo <- reco_catnat %>% 
  distinct(codgeo)
start_date <- as.Date("2003-01-01")
end_date <- as.Date("2022-12-31")
saison_dates <- seq.Date(from = start_date, to = end_date, by = "quarter")

compte_catnat <- expand.grid(codgeo = unique_codgeo$codgeo, 
                           saison = saison_dates) %>%
  left_join(reco_catnat, by = c('codgeo', 'saison' = 'saison_arrete')) %>% 
  mutate(
    reconnaissance_2003 =  replace_na(reconnaissance_2003, 0),
    reconnaissance_2010 = replace_na(reconnaissance_2010, 0),
  ) %>% 
  group_by(codgeo) %>% 
  arrange(saison) %>% 
  mutate(
    reco_catnat_since_2003 = cumsum(reconnaissance_2003 == 1),
    reco_catnat_since_2010 = cumsum(reconnaissance_2010 == 1)
  ) %>% 
  ungroup() %>% 
  filter(lubridate::year(saison)>=2010) 

# Ne semble pas tout à fait conforme à la version depuis 2018. A creuser en cas de reprise.

df_regression <- df_regression %>% 
  left_join(compte_catnat %>% select(codgeo, saison, reco_catnat_since_2003, reco_catnat_since_2010), by = c('codgeo', 'saison')) %>% 
  mutate(reco_catnat_since_2003 =  replace_na(reco_catnat_since_2003, 0),
         reco_catnat_since_2010 = replace_na(reco_catnat_since_2010, 0))

# Traitement à la commune, et ne varie pas pour tout le monde sur la durée -> dep FE ?
# Bof : ensuite c'est très bruyant. Sur longue période, on conserve la commune

df_depuis_2018 <- df_regression %>% filter(anneemut >=2018) %>% mutate(reco_squared = reco_catnat_up_to_t^2)

## 3.2. OLS

# Ici, on fait une regression en dep FE, pour montrer qu'on va aux fraises. Pour le reste, simplement le nombre cumulé de reco

fOLS_reco_codgeo <-log_valeur_fonciere ~   reco_catnat_since_2003*RGA_moyen_fort+
  ## Maisons
  log_surface_batie + log_surface_parcelle +
  Trois_pieces_et_moins+ Cinq_pieces+ Six_pieces+ Sept_pieces_et_plus+ #omise = 4 pièces
  Recent+P_1914+P1914_1944+P1945_1960+P1961_1974+P1975_1989+P2013_+ #omise = P1990_2012
  Deux_niveaux_et_plus+ #omise = 1 niveau
  Sans_SdB+Deux_SdB_et_plus+ #omise = 1 SDB
  Sans_garage+Deux_garages_et_plus+ #omise = 1 garage
  Terrasse+ Piscine+ Autre_dependance +
  ## Iris
  tx_chom_LD + part_3039 +part_couple_enf +part_log_vac+dens_log +
  part_res_sec+ rap_inter_dec+ mediane_niv_vie+ part_etudiants+
  ## 200m
  distance_college + taux_mentions_sur_inscrits+
  Gare_500m+Gare_3km+ Gare_3km_5km+ Gare_5km_7km+ Gare_7km_10km+ #omise = Gare_p10km
  Metro_tram_300m + Metro_tram_300m_600m + Metro_tram_600m_1k + #omise = >1km
  Sevesosh_1km+ Sevesosh_1km_10km+ #omise = Sevesosh_p10km
  Appartenance_QPV+ QPV_500m+ # omise QPV_p500m_ou_non
  PEB_A+ PEB_B+ PEB_C+ PEB_D+ #omise = pas de PEB (peut être faut il créer la variable)
  cote_300m+cote_300m_1km | codgeo  +  anneemut #omise = cote_p1km

fOLS_reco_codgeo_2018 <-log_valeur_fonciere ~   reco_catnat_up_to_t*RGA_moyen_fort+
  ## Maisons
  log_surface_batie + log_surface_parcelle +
  Trois_pieces_et_moins+ Cinq_pieces+ Six_pieces+ Sept_pieces_et_plus+ #omise = 4 pièces
  Recent+P_1914+P1914_1944+P1945_1960+P1961_1974+P1975_1989+P2013_+ #omise = P1990_2012
  Deux_niveaux_et_plus+ #omise = 1 niveau
  Sans_SdB+Deux_SdB_et_plus+ #omise = 1 SDB
  Sans_garage+Deux_garages_et_plus+ #omise = 1 garage
  Terrasse+ Piscine+ Autre_dependance +
  ## Iris
  tx_chom_LD + part_3039 +part_couple_enf +part_log_vac+dens_log +
  part_res_sec+ rap_inter_dec+ mediane_niv_vie+ part_etudiants+
  ## 200m
  distance_college + taux_mentions_sur_inscrits+
  Gare_500m+Gare_3km+ Gare_3km_5km+ Gare_5km_7km+ Gare_7km_10km+ #omise = Gare_p10km
  Metro_tram_300m + Metro_tram_300m_600m + Metro_tram_600m_1k + #omise = >1km
  Sevesosh_1km+ Sevesosh_1km_10km+ #omise = Sevesosh_p10km
  Appartenance_QPV+ QPV_500m+ # omise QPV_p500m_ou_non
  PEB_A+ PEB_B+ PEB_C+ PEB_D+ #omise = pas de PEB (peut être faut il créer la variable)
  cote_300m+cote_300m_1km | codgeo  +  anneemut #omise = cote_p1km

fOLS_reco_dep <-log_valeur_fonciere ~   reco_catnat_since_2003*RGA_moyen_fort+
  ## Maisons
  log_surface_batie + log_surface_parcelle +
  Trois_pieces_et_moins+ Cinq_pieces+ Six_pieces+ Sept_pieces_et_plus+ #omise = 4 pièces
  Recent+P_1914+P1914_1944+P1945_1960+P1961_1974+P1975_1989+P2013_+ #omise = P1990_2012
  Deux_niveaux_et_plus+ #omise = 1 niveau
  Sans_SdB+Deux_SdB_et_plus+ #omise = 1 SDB
  Sans_garage+Deux_garages_et_plus+ #omise = 1 garage
  Terrasse+ Piscine+ Autre_dependance +
  ## Iris
  tx_chom_LD + part_3039 +part_couple_enf +part_log_vac+dens_log +
  part_res_sec+ rap_inter_dec+ mediane_niv_vie+ part_etudiants+
  ## 200m
  distance_college + taux_mentions_sur_inscrits+
  Gare_500m+Gare_3km+ Gare_3km_5km+ Gare_5km_7km+ Gare_7km_10km+ #omise = Gare_p10km
  Metro_tram_300m + Metro_tram_300m_600m + Metro_tram_600m_1k + #omise = >1km
  Sevesosh_1km+ Sevesosh_1km_10km+ #omise = Sevesosh_p10km
  Appartenance_QPV+ QPV_500m+ # omise QPV_p500m_ou_non
  PEB_A+ PEB_B+ PEB_C+ PEB_D+ #omise = pas de PEB (peut être faut il créer la variable)
  cote_300m+cote_300m_1km  + #omise = côte à +10km
  ## Celles parce qu'on n'a plus de commune FE
  part_log_soc + nb_log_total | dep + anneemut

OLS_reco_codgeo <- fixest::feols(fOLS_reco_codgeo, data = df_regression, cluster = ~ codgeo)
OLS_reco_dep <- fixest::feols(fOLS_reco_dep, data = df_regression, cluster = ~ codgeo)
OLS_reco_codgeo_2018 <- fixest::feols(fOLS_reco_codgeo_2018, data =df_depuis_2018, cluster = ~ codgeo)

summary(OLS_reco_codgeo)
summary(OLS_reco_dep) #commenter qu'on voit que le RGA fout le camp

etable(list(OLS_reco_codgeo, OLS_reco_dep, OLS_reco_codgeo_2018), 
       tex = TRUE, 
       dict = myDict, 
       title = 'In-pricing of shrink-swell insurance in French house prices (2010-2022): OLS',
       notes = "Source: Metropolitain France house sales (2010-2022), DV3F; enriched by Insee PSAR-SL; Shrink swell exposure maps (BRGM); BD CatNat", 
       digits = 3,
       fontsize = 'small',
       se.below = FALSE,
       coefstat = "se",
       keep = c('Exposed to shrink-swell', 'Number of past CatNat recognitions (2003)','Number of past CatNat recognitions (2018)'), 
       extralines = list("^House characteristics" = c("Yes", "Yes","Yes"),
                         "^Neighbourhood characteristics" = c("Yes", "Yes","Yes"),
                         "^Local amenities" = c("Yes", "Yes","Yes")
       ), 
       adjustbox = TRUE
)

#  Préciser dans l'interprétation qu'on ne peut capturer qu'un effet local
# (dans le sens où si en été 2020 toutes les communes commencent à pricer les reco
# catnat davantage, on ne va pas le capturer, on n'aura que l'effet marginal pour 
# celles dont la reco catnat change)

# Pas d'influence de termes quadratiques. 
# Pas d'influence sans intéraction. 

############################################
###### 4. Price-in de CatNat (arrêté) ######
############################################

# Dans chaque cas, je veux 1) comparer les 1-2 et les 3-4, ou les 2 et les 3 (mais tous en RGA); idée = commune est le dommage
# Puis 2) comparer parmi les reconnus, les RGA et les pas RGA 

# Dans les deux cas, il faudra a) montrer l'effet post, puis b) tester les pretrends et c) share of retreatment

# 4.1. Bases
df_regression_arrow <- df_regression |> 
  mutate(saison_arrete = as.Date(saison_arrete), 
         saison= as.Date(saison))|>
  compute()|>
  arrow::as_arrow_table()

nombre_transac_par_saison <- df_regression_arrow |> 
  filter(anneemut>=2018)|>
  to_duckdb()|>
  group_by(saison_arrete) |> 
  summarize(length = n_distinct(idmutation)) |>
  to_arrow()|>
  collect()|>
  arrange(saison_arrete)

# Ca fait 9 saisons avec au moins 10K transactions MAIS c'est un mauvais indicateur : ce qui compte c'est le nbr de transactions 
# dans l'ensemble des saisons autour. Ici, il me faudrait faire un tableau avec le nbr de refus / demande par trimestre, en termes de date 
# d'arrêtés, puis regarder le nbr de transactions auquel ça correspond. Pour l'instant, on empile, car échec sur une saison.

## Transformation en durée relative
# Le principe est de créer un df par saison d'arrêté, créer des temporalités relatives et sélectionnant dans chaque cas
# un groupe de traitement et un groupe de contrôle
saison_df <- df_regression_arrow |> select(saison_arrete)|> distinct() |> collect()

# Les saisons qui nous intéressent : toutes jusqu'en 2022 (où l'on exclura les dernières en aval) -> 14 saisons d'arrêtés d'intérêt
saisons <- as.Date(saison_df$saison_arrete[!is.na(saison_df$saison_arrete) & lubridate::year(saison_df$saison_arrete) <= 2022])
saisons <- sort(saisons)

list_filtered_data <- list()

# Iterate over the indices of the unique seasons
for (i in seq_along(saisons)) {
  saison_ref <- as.Date(saisons[i])
  # On constitue des les groupes sur une saison d'intérêt :
  subset_saison <- df_regression_arrow |>
    filter(saison_arrete == saison_ref)|>
    compute()
  codes_treated <- subset_saison |>
    filter(rang_circ_2019 <= 2 &rang_circ_2019 >=1 & demande == 1) |> 
    select(codgeo) |>
    collect()|> 
    distinct()
  
  codes_untreated <- subset_saison |>
    filter(rang_circ_2019 >2 & rang_circ_2019 <=4 & demande == 1) |> 
    select(codgeo) |>
    collect()|>
    distinct()
  
  # On filtre un df sur nos 2 groupes
  filtered_data <- df_regression_arrow |>
    filter(codgeo %in% codes_treated$codgeo | codgeo %in% codes_untreated$codgeo) |>
    collect()
  
  # On crée les dummies/variables souhaitées : treatment, saison_ref, time
  filtered_data$treatment <- ifelse(filtered_data$codgeo %in% codes_treated$codgeo, 1, 0)
  filtered_data$utter_treatment <- case_when(
    filtered_data$treatment==1 & filtered_data$RGA_moyen_fort==1 ~ 1,
    filtered_data$treatment==0 & filtered_data$RGA_moyen_fort==1 ~ 0,
    TRUE~NA)
  filtered_data$hurt <- case_when(
    filtered_data$treatment==1 & filtered_data$RGA_moyen_fort==1 ~ 1,
    filtered_data$treatment==1 & filtered_data$RGA_moyen_fort==0 ~ 0,
    TRUE~NA)
  filtered_data$saison_ref <- saison_ref
  filtered_data$time <- round(as.numeric(difftime(filtered_data$saison, saison_ref, units = "weeks")) / 13, 0)
  filtered_data$post <- ifelse(filtered_data$time >= 0,1,0)
  filtered_data$pre <- ifelse(filtered_data$time < 0,1,0)
  
  # Store the filtered data in the list if it contains more than one row
  if (nrow(filtered_data) > 1) {
    list_filtered_data[[as.character(saison_ref)]] <- filtered_data
  }
}

final_combined_df <- do.call(rbind, list_filtered_data)
final_combined_df$time <- as.factor(final_combined_df$time)
final_combined_df$time <- relevel(factor(final_combined_df$time), ref = '-1')
levels(final_combined_df$time)

final_combined_df <- final_combined_df %>% 
  mutate(time_modified = as.factor(case_when(
    as.numeric(as.character(time)) <= -1 & as.numeric(as.character(time)) >= -4 ~ -1,
    as.numeric(as.character(time)) <= -5 & as.numeric(as.character(time)) >= -8 ~ -2,
    as.numeric(as.character(time)) <= -9 & as.numeric(as.character(time)) >= -12 ~ -3,
    as.numeric(as.character(time)) <= -13 & as.numeric(as.character(time)) >= -16 ~ -4,
    as.numeric(as.character(time)) <= -17 & as.numeric(as.character(time)) >= -20 ~ -5,
    as.numeric(as.character(time)) >= 0 & as.numeric(as.character(time)) <= 3 ~ 0,
    as.numeric(as.character(time)) >= 4 & as.numeric(as.character(time)) <= 7 ~ 1,
    as.numeric(as.character(time)) >= 8 & as.numeric(as.character(time)) <= 11 ~ 2,
    as.numeric(as.character(time)) >= 12 & as.numeric(as.character(time)) <= 15 ~ 3,
    TRUE ~ NA_real_ # Handle cases outside the specified range
  ))
  ) %>% 
  filter(!is.na(time_modified))
#final_combined_df$time_modified <- relevel(final_combined_df$time_modified, ref = '-1')
#levels(final_combined_df$time_modified)

## 4.2. Nos deux DiD
fstacked_treatment <- log_valeur_fonciere ~ i(time_modified, treatment, ref = '-1')+ #adding treatment keeps results but decreases precision
  # (normal, considering codgeo FE)
  ## Maisons
  log_surface_batie + log_surface_parcelle +
  Trois_pieces_et_moins+ Cinq_pieces+ Six_pieces+ Sept_pieces_et_plus+ #omise = 4 pièces
  Recent+P_1914+P1914_1944+P1945_1960+P1961_1974+P1975_1989+P2013_+ #omise = P1990_2012
  Deux_niveaux_et_plus+ #omise = 1 niveau
  Sans_SdB+Deux_SdB_et_plus+ #omise = 1 SDB
  Sans_garage+Deux_garages_et_plus+ #omise = 1 garage
  Terrasse+ Piscine+ Autre_dependance +
  ## Iris
  tx_chom_LD + part_3039 +part_couple_enf +part_log_vac+dens_log +
  part_res_sec+ rap_inter_dec+ mediane_niv_vie+ part_etudiants+
  ## 200m
  distance_college + taux_mentions_sur_inscrits+
  Gare_500m+Gare_3km+ Gare_3km_5km+ Gare_5km_7km+ Gare_7km_10km+ #omise = Gare_p10km
  Metro_tram_300m + Metro_tram_300m_600m + Metro_tram_600m_1k + #omise = >1km
  Sevesosh_1km+ Sevesosh_1km_10km+ #omise = Sevesosh_p10km
  Appartenance_QPV+ QPV_500m+ # omise QPV_p500m_ou_non
  PEB_A+ PEB_B+ PEB_C+ PEB_D+ #omise = pas de PEB (peut être faut il créer la variable)
  cote_300m+cote_300m_1km   | codgeo + anneemut # omise = côte à +10km

fstacked_utter_treatment <-  log_valeur_fonciere ~ i(time_modified, treatment, ref = '-1')+
  ## Maisons
  log_surface_batie + log_surface_parcelle +
  Trois_pieces_et_moins+ Cinq_pieces+ Six_pieces+ Sept_pieces_et_plus+ #omise = 4 pièces
  Recent+P_1914+P1914_1944+P1945_1960+P1961_1974+P1975_1989+P2013_+ #omise = P1990_2012
  Deux_niveaux_et_plus+ #omise = 1 niveau
  Sans_SdB+Deux_SdB_et_plus+ #omise = 1 SDB
  Sans_garage+Deux_garages_et_plus+ #omise = 1 garage
  Terrasse+ Piscine+ Autre_dependance +
  ## Iris
  tx_chom_LD + part_3039 +part_couple_enf +part_log_vac+dens_log +
  part_res_sec+ rap_inter_dec+ mediane_niv_vie+ part_etudiants+
  ## 200m
  distance_college + taux_mentions_sur_inscrits+
  Gare_500m+Gare_3km+ Gare_3km_5km+ Gare_5km_7km+ Gare_7km_10km+ #omise = Gare_p10km
  Metro_tram_300m + Metro_tram_300m_600m + Metro_tram_600m_1k + #omise = >1km
  Sevesosh_1km+ Sevesosh_1km_10km+ #omise = Sevesosh_p10km
  Appartenance_QPV+ QPV_500m+ # omise QPV_p500m_ou_non
  PEB_A+ PEB_B+ PEB_C+ PEB_D+ #omise = pas de PEB (peut être faut il créer la variable)
  cote_300m+cote_300m_1km   | codgeo + anneemut # omise = côte à +10km

stacked_treatment <- fixest::feols(fstacked_treatment, 
                                   data = final_combined_df, 
                                   cluster = ~ codgeo)
stacked_utter_treatment <- fixest::feols(fstacked_utter_treatment, 
                                   data = final_combined_df %>% filter(!is.na(utter_treatment)), 
                                   cluster = ~ codgeo)

etable(list(stacked_treatment, stacked_utter_treatment), 
       tex = TRUE, 
       dict = myDict, 
       title = 'In-pricing of shrink-swell insurance in French house prices (2010-2022)',
       notes = "Source: Metropolitain France house sales (2010-2022), DV3F; enriched by Insee PSAR-SL; Shrink swell exposure maps (BRGM); Soil Wetness Index (Météo France)", 
       digits = 3,
       fontsize = 'small',
       se.below = FALSE,
       coefstat = "se",
       keep = c('Years to treatment', 'Treatment (return period x clay)'), 
       extralines = list("^House characteristics" = c("Yes","Yes"),
                         "^Neighbourhood characteristics" = c("Yes","Yes"),
                         "^Local amenities" = c("Yes","Yes")
       )
)

## 4.3. Les CI
png(paste0(path_outputs, "Stacked_treatment_95CI.png"), width = 1000, height = 400)
fixest::iplot(stacked_treatment,
              ci_level = 0.95,
              main = "Impact of CatNat recognition through time",  # No title
              xlab = "Year to treatment",
              col = raw_sienna, 
              cex = 1.5)
dev.off()

png(paste0(path_outputs, "Stacked_utter_treatment_95CI.png"), width = 1000, height = 400)
fixest::iplot(stacked_utter_treatment,
              ci_level = 0.95,
              main = "Filtered on houses at risk",  # No title
              xlab = "Year to treatment",
              col = midnight_blue,
              cex = 1.5)
dev.off()

# On y ajoute une version sans clustering de se
stacked_treatment <- fixest::feols(fstacked_treatment, 
                                   data = final_combined_df, 
                                   vcov = 'hetero')
stacked_utter_treatment <- fixest::feols(fstacked_utter_treatment, 
                                         data = final_combined_df %>% filter(!is.na(utter_treatment)), 
                                         vcov = 'hetero')

png(paste0(path_outputs, "Stacked_treatment_95CI_HETERO.png"), width = 1000, height = 400)
fixest::iplot(stacked_treatment,
              ci_level = 0.95,
              main = "Impact of CatNat recognition through time",  # No title
              xlab = "Year to treatment",
              col = raw_sienna, 
              cex = 1.5)
dev.off()

png(paste0(path_outputs, "Stacked_utter_treatment_95CI_HETERO.png"), width = 1000, height = 400)
fixest::iplot(stacked_utter_treatment,
              ci_level = 0.95,
              main = "Filtered on houses at risk",  # No title
              xlab = "Year to treatment",
              col = midnight_blue,
              cex = 1.5)
dev.off()

## 4.4. On essaye de voir l'effet du RGA parmi les traités : ça ne donne pas grand chose..;

# For now, i'm using this one, and i'll comment on how wobbly it is 
fstacked_hurt <-  log_valeur_fonciere ~ i(time_modified,RGA_moyen_fort)+
  factor(time_modified)+
  ## Maisons
  log_surface_batie + log_surface_parcelle +
  Trois_pieces_et_moins+ Cinq_pieces+ Six_pieces+ Sept_pieces_et_plus+ #omise = 4 pièces
  Recent+P_1914+P1914_1944+P1945_1960+P1961_1974+P1975_1989+P2013_+ #omise = P1990_2012
  Deux_niveaux_et_plus+ #omise = 1 niveau
  Sans_SdB+Deux_SdB_et_plus+ #omise = 1 SDB
  Sans_garage+Deux_garages_et_plus+ #omise = 1 garage
  Terrasse+ Piscine+ Autre_dependance +
  ## Iris
  tx_chom_LD + part_3039 +part_couple_enf +part_log_vac+dens_log +
  part_res_sec+ rap_inter_dec+ mediane_niv_vie+ part_etudiants+
  ## 200m
  distance_college + taux_mentions_sur_inscrits+
  Gare_500m+Gare_3km+ Gare_3km_5km+ Gare_5km_7km+ Gare_7km_10km+ #omise = Gare_p10km
  Metro_tram_300m + Metro_tram_300m_600m + Metro_tram_600m_1k + #omise = >1km
  Sevesosh_1km+ Sevesosh_1km_10km+ #omise = Sevesosh_p10km
  Appartenance_QPV+ QPV_500m+ # omise QPV_p500m_ou_non
  PEB_A+ PEB_B+ PEB_C+ PEB_D+ #omise = pas de PEB (peut être faut il créer la variable)
  cote_300m+cote_300m_1km   | codgeo  # omise = côte à +10km

stacked_hurt_treated <- fixest::feols(fstacked_hurt, 
                              data = final_combined_df%>% filter(treatment==1), 
                              cluster = ~ codgeo)
summary(stacked_hurt)
stacked_hurt_untreated <- fixest::feols(fstacked_hurt, 
                              data = final_combined_df %>% filter(treatment==0), 
                              cluster = ~ codgeo)

iplot(stacked_hurt_treated) 
iplot(stacked_hurt_untreated) 

## Pb : multicol when i add baseline rga

## 4.5. Graphe répétition des traitements pour chaque groupe

df_retreatments <- final_combined_df  %>% 
  filter(!is.na(time_modified)) %>% 
  group_by(treatment, time_modified) %>% 
  summarize(average_past_treatment = mean(reco_catnat_since_2010))


plot_rep <- ggplot(df_retreatments, aes(x = time_modified, y = average_past_treatment, color = as.factor(treatment))) +
  geom_line() +
  geom_point() +
  labs(
    title = "The issue of repeated treatment: are transactions comparable across groups?",
    x = "Years to treatment",
    y = "Average cumulative recognitions since 2010",
    color = "Treatment Group",
    caption = str_wrap("Data: French house sales (DV3F). Return periods are computed using Météo France SWIs. ", width = 160),
  ) +
  scale_color_manual(values = c("0" = midnight_blue, "1" = raw_sienna), 
                     breaks = c("1", "0"), 
                     labels = c("0" = 'Untreated (return period 10-25)',
                                "1" = 'Treated (return period >25)'), 
                     name = NULL) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1),
        legend.position = "top",
        plot.caption = element_text(hjust = 0))

plot_rep
ggsave(paste0(path_outputs, 'stacked_repeated_treatment.png'), plot = plot_rep, width = 900/72, height = 425/72, dpi = 72)

## 4.6 Graphique de prix de chaque groupes
density_plot <- ggplot(final_combined_df %>% 
                         filter(!is.na(time_modified) & Valeur_fonciere < 1500000),  # Filter at 1.5 million
                       aes(x = Valeur_fonciere, fill = factor(treatment))) +
  geom_density(alpha = 0.5) +
  labs(
    title = "Distribution of House prices across groups",
    x = "House price (truncated at 1.5 million euros)",  # Rename x axis
    y = "Density",
    caption = str_wrap("Data: French house sales (DV3F). Return periods are computed using Météo France SWIs. ", width = 160)  # Note about filtering
  ) +
  scale_fill_manual(
    values = c("0" = midnight_blue, "1" = raw_sienna), 
    breaks = c("1", "0"), 
    labels = c("0" = 'Untreated (return period 10-25)', 
               "1" = 'Treated (return period >25)'), 
    name = NULL
  ) +
  scale_y_continuous(labels = NULL) +  
  scale_x_continuous(labels = scales::comma) +  # Remove scientific notation and use comma format
  theme_minimal() +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1),
    legend.position = "top",
    plot.caption = element_text(hjust = 0)
  )


density_plot
ggsave(paste0(path_outputs, 'stacked_price_distribution.pdf'), plot = density_plot, width = 900/72, height = 425/72, dpi = 72)

## 4.7 Tableau de balance sur les variables numériques
numerical_columns <- final_combined_df %>%
  ungroup() %>%
  select(Valeur_fonciere, Surface_batie, Surface_parcelle, 
         part_log_soc, nb_log_total, distance_college, tx_chom_LD,
         dens_log, part_3039, part_couple_enf,
         taux_mentions_sur_inscrits, part_etudiants, 
         part_log_vac, part_res_sec, mediane_niv_vie, rap_inter_dec)
numerical_columns <- names(numerical_columns)

# Combining the treatment column and the selected numerical columns
df_to_summarize <- final_combined_df %>%
  select(treatment, numerical_columns)

# Calculating the average per group
options(scipen=999)
df_summary <- df_to_summarize %>%
  group_by(treatment) %>%
  summarise(across(everything(), mean, na.rm = TRUE)) %>% 
  mutate(dens_log = as.numeric(dens_log))

df_summary <- df_to_summarize %>%
  group_by(treatment) %>%
  summarise(across(everything(), mean, na.rm = TRUE)) %>% 
  mutate(dens_log = as.numeric(dens_log)) %>% 
  rename("Property Value" = Valeur_fonciere,
         "Built-up Area" = Surface_batie,
         "Plot Area" = Surface_parcelle,
         "Share of social housing" = part_log_soc,
         "Number of dwellings in municipality" = nb_log_total,
         "Distance to middle-school (km)" = distance_college,
         "Long-term unemployment rate" = tx_chom_LD,
         "Density of housing (/km2)" = dens_log,
         "Share of population aged 30-39" = part_3039,
         "Share of couples with children" = part_couple_enf,
         "Share of middle schoolers achieving honours" = taux_mentions_sur_inscrits,
         "Share of students" = part_etudiants,
         "Share of vacant dwellings" = part_log_vac,
         "Share of secondary residence" = part_res_sec,
         "Median living standard (euros)" = mediane_niv_vie,
         "Interdecile ratio" = rap_inter_dec)

t_tests_df <- df_to_summarize %>%
  mutate(dens_log = as.numeric(as.character(dens_log)))

t_tests_df <- t_tests_df%>% 
  summarise(across(numerical_columns, 
                   ~ t.test(. ~ treatment, data = t_tests_df)$p.value)) %>%
  pivot_longer(everything(), names_to = "Variable", values_to = "p_value")
#They're all significant hihi

custom_round <- function(x) {
  if (x < 100) {
    round(x, 1)  # Round to 1 digit
  } else if (x >= 100 & x < 1000) {
    round(x, 0)  # Round to 0 digits
  } else {
    round(x, -1) # Round to -1 digits (nearest 10)
  }
}
# Vectorize the custom rounding function
custom_round_vec <- Vectorize(custom_round)

df_summary_transposed <- df_summary %>%
  pivot_longer(-treatment, names_to = "Variable", values_to = "Value") %>%
  pivot_wider(names_from = treatment, values_from = Value) %>% 
  mutate(across(where(is.numeric),custom_round_vec))

# Generating the LaTeX table
latex_table <- kable(df_summary_transposed, format = "latex", booktabs = TRUE, 
                     caption = "Average Values per Treatment Status",
                     col.names = c("Variable", "Treated", "Control")) %>%
  kable_styling(latex_options = c("hold_position"))


# Output the LaTeX code
cat(latex_table)

final_combined_df %>% group_by(treatment) %>% summarize(Municipalities = n_distinct(codgeo),
                                                        Unique_transactions = n_distinct(idmutation),
                                                        Transactions = n())

# We add, à la main bc i'm late, the number of observations and municipalities

## 4.8 Corrected TWFE

## Je n'ai pas l'impression que ça fonctionne: ce n'est pas un traitement graduel / échelonné, mais pour chaque saison une DiD
## simple. L'idéal serait donc de commencer par une différenc simple, tbh.

## En l'état, ce code n'est pas adapté: je le laisse là en cas de vélléité de reprendre.
library(etwfe)

mod =
  etwfe(
    fml  = lemp ~ lpop, # outcome ~ controls
    tvar = year,        # time variable
    gvar = first.treat, # group variable
    data = mpdta,       # dataset
    vcov = ~countyreal  # vcov adjustment (here: clustered)
  )

#Then ATT : 
emfx(mod)
emfx(mod, type = "event")

hmod = etwfe(
  lemp ~ lpop, tvar = year, gvar = first.treat, data = mpdta, 
  vcov = ~countyreal,
  xvar = gls           ## <= het. TEs by gls, où ici je mettrais treatment ? confusing
)

fsun_ab_stacked <- log_valeur_fonciere ~ sunab(cohort = lubridate::year(saison_ref),
                                                  period = time_modified)+ 
  ## Maisons
  log_surface_batie + log_surface_parcelle +
  Trois_pieces_et_moins+ Cinq_pieces+ Six_pieces+ Sept_pieces_et_plus+ #omise = 4 pièces
  Recent+P_1914+P1914_1944+P1945_1960+P1961_1974+P1975_1989+P2013_+ #omise = P1990_2012
  Deux_niveaux_et_plus+ #omise = 1 niveau
  Sans_SdB+Deux_SdB_et_plus+ #omise = 1 SDB
  Sans_garage+Deux_garages_et_plus+ #omise = 1 garage
  Terrasse+ Piscine+ Autre_dependance +
  ## Iris
  tx_chom_LD + part_3039 +part_couple_enf +part_log_vac+dens_log +
  part_res_sec+ rap_inter_dec+ mediane_niv_vie+ part_etudiants+
  ## 200m
  distance_college + taux_mentions_sur_inscrits+
  Gare_500m+Gare_3km+ Gare_3km_5km+ Gare_5km_7km+ Gare_7km_10km+ #omise = Gare_p10km
  Metro_tram_300m + Metro_tram_300m_600m + Metro_tram_600m_1k + #omise = >1km
  Sevesosh_1km+ Sevesosh_1km_10km+ #omise = Sevesosh_p10km
  Appartenance_QPV+ QPV_500m+ # omise QPV_p500m_ou_non
  PEB_A+ PEB_B+ PEB_C+ PEB_D+ #omise = pas de PEB (peut être faut il créer la variable)
  cote_300m+cote_300m_1km   | codgeo + anneemut # omise = côte à +10km

sun_ab_stacked <- fixest::feols(fsun_ab_stacked, 
                                   data = final_combined_df, 
                                   cluster= ~ codgeo)

etable(sun_ab_stacked)


## 4.9 Honest DiD : même commentaire, je pense qu'il est mal adapté, mais ça montre que c'est wobbly
### Corriger pour 5-3
coefficients <- summary(stacked_treatment)$coefficients
betahat <- coefficients[grep("time_modified", names(coefficients)) ]#save the coefficients

# Save the cov matrix
cov_matrix <- summary(stacked_treatment)$cov.scaled
sigma <- cov_matrix[grep("time_modified", rownames(cov_matrix)), grep("time_modified", colnames(cov_matrix))]

originalResults <- HonestDiD::constructOriginalCS(betahat = betahat,
                                                  sigma = sigma,
                                                  numPrePeriods = 4,
                                                  numPostPeriods = 4)
delta_rm_results <-
  HonestDiD::createSensitivityResults_relativeMagnitudes(
    betahat = betahat, #coefficients
    sigma = sigma, #covariance matrix
    numPrePeriods = 4, #num. of pre-treatment coefs
    numPostPeriods = 4, #num. of post-treatment coefs
    Mbarvec = seq(0.5,2,by=0.5) #values of Mbar
  )

HonestDiD::createSensitivityPlot_relativeMagnitudes(delta_rm_results, originalResults)
