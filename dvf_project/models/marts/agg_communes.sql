{{ config(materialized='table') }}

SELECT
    code_commune,
    commune,
    code_departement,
    annee,

-- Métriques de volume
COUNT(*) AS nb_transactions,

-- Prix
ROUND(MEDIAN (valeur_fonciere), 0) AS prix_median,
ROUND(AVG(valeur_fonciere), 0) AS prix_moyen,
ROUND(MIN(valeur_fonciere), 0) AS prix_min,
ROUND(MAX(valeur_fonciere), 0) AS prix_max,

-- Prix au m² (pour biens avec surface)
ROUND(MEDIAN (prix_m2), 0) AS prix_m2_median,
ROUND(AVG(prix_m2), 0) AS prix_m2_moyen,

-- Surfaces
ROUND(
    MEDIAN (surface_reelle_bati),
    1
) AS surface_mediane,
ROUND(AVG(surface_reelle_bati), 1) AS surface_moyenne,

-- Répartition type de bien
SUM(CASE WHEN type_local = 'Maison' THEN 1 ELSE 0 END) AS nb_maisons,
    SUM(CASE WHEN type_local = 'Appartement' THEN 1 ELSE 0 END) AS nb_appartements

FROM {{ ref('fct_transactions') }}

WHERE has_prix = 1

GROUP BY code_commune, commune, code_departement, annee