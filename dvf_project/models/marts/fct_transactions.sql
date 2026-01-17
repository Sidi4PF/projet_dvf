{{ config(materialized='table') }}

SELECT
    identifiant_de_document,
    date_mutation,
    EXTRACT(
        YEAR
        FROM date_mutation
    ) AS annee,
    EXTRACT(
        MONTH
        FROM date_mutation
    ) AS mois,
    EXTRACT(
        QUARTER
        FROM date_mutation
    ) AS trimestre,
    nature_mutation,
    valeur_fonciere,

-- Localisation
code_postal, commune, code_departement, code_commune,

-- Bien
type_local,
surface_reelle_bati,
nombre_pieces_principales,
surface_terrain,
nombre_de_lots,

-- Prix au m² (si surface existe)
CASE
    WHEN surface_reelle_bati > 0 THEN valeur_fonciere / surface_reelle_bati
    ELSE NULL
END AS prix_m2,

-- Flags de qualité
CASE WHEN valeur_fonciere > 0 THEN 1 ELSE 0 END AS has_prix,
    CASE WHEN surface_reelle_bati > 0 THEN 1 ELSE 0 END AS has_surface,
    
    source_file,
    loaded_at

FROM {{ ref('stg_dvf__mutations') }}

WHERE 1=1
    AND nature_mutation = 'Vente'  -- Focus sur les ventes
    AND valeur_fonciere > 1000     -- Exclure prix aberrants
    AND valeur_fonciere < 50000000 -- Exclure valeurs extrêmes
    AND type_local IN ('Maison', 'Appartement', 'Local industriel. commercial ou assimilé')