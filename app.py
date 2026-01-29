import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(
    page_title="DVF - Marché Immobilier Français",
    layout="wide"
)

@st.cache_data
def load_data():
    fct = pd.read_csv("data/processed/fct_transactions_sample.csv")
    agg = pd.read_csv("data/processed/agg_communes.csv")
    fct["DATE_MUTATION"] = pd.to_datetime(fct["DATE_MUTATION"])
    fct["ANNEE"] = fct["ANNEE"].astype(int)
    agg["ANNEE"] = agg["ANNEE"].astype(int)
    return fct, agg

fct, agg = load_data()

# --- Sidebar ---
st.sidebar.header("Filtres")
annees = sorted(fct["ANNEE"].unique())
annee_sel = st.sidebar.multiselect("Année", annees, default=annees)

depts = sorted(fct["CODE_DEPARTEMENT"].dropna().unique())
dept_sel = st.sidebar.multiselect("Département", depts, default=[])

type_bien = st.sidebar.multiselect(
    "Type de bien",
    ["Maison", "Appartement"],
    default=["Maison", "Appartement"]
)

st.sidebar.divider()
st.sidebar.caption("Données : DVF (DGFiP) via data.gouv.fr")
st.sidebar.caption("Pipeline : Python / Snowflake / dbt Core")
st.sidebar.caption("Echantillon : ~10% des transactions résidentielles 2020-2023")

# --- Filtrage ---
fct_f = fct[
    (fct["ANNEE"].isin(annee_sel)) &
    (fct["TYPE_LOCAL"].isin(type_bien)) &
    (fct["PRIX_M2"].notna()) &
    (fct["PRIX_M2"] > 0)
]
if dept_sel:
    fct_f = fct_f[fct_f["CODE_DEPARTEMENT"].isin(dept_sel)]

agg_f = agg[
    (agg["ANNEE"].isin(annee_sel)) &
    (agg["NB_TRANSACTIONS"] > 100) &
    (agg["PRIX_M2_MEDIAN"].notna()) &
    (agg["PRIX_M2_MEDIAN"] > 0) &
    (agg["PRIX_M2_MEDIAN"] < 20000)
]
agg_f = agg_f[~agg_f["COMMUNE"].isin(["LUNERAY", "CAULNES", "LONGUEIL ANNEL"])]
if dept_sel:
    agg_f = agg_f[agg_f["CODE_DEPARTEMENT"].isin(dept_sel)]

# --- Deltas année précédente ---
def get_delta(col, annee_courante, annee_prec):
    val_c = fct_f[fct_f["ANNEE"] == annee_courante][col].median()
    val_p = fct_f[fct_f["ANNEE"] == annee_prec][col].median()
    if pd.isna(val_c) or pd.isna(val_p) or val_p == 0:
        return None
    return f"{((val_c - val_p) / val_p * 100):+.1f}% vs {annee_prec}"

annee_max = max(annee_sel) if annee_sel else 2023
annee_prec = annee_max - 1

# --- Header ---
st.title("Marché Immobilier Français - DVF 2020-2023")
st.caption("Transactions résidentielles (maisons et appartements) — source DGFiP")

st.divider()

# --- KPIs ---
st.subheader("Vue nationale")
col1, col2, col3, col4 = st.columns(4)

nb_trans = len(fct_f)
prix_m2_med = fct_f["PRIX_M2"].median()
prix_med = fct_f["VALEUR_FONCIERE"].median()
nb_communes = fct_f["COMMUNE"].nunique()

delta_prix_m2 = get_delta("PRIX_M2", annee_max, annee_prec)
delta_prix = get_delta("VALEUR_FONCIERE", annee_max, annee_prec)

col1.metric("Transactions analysées", f"{nb_trans:,}")
col2.metric("Prix médian", f"{int(prix_med):,} €", delta=delta_prix)
col3.metric("Prix/m² médian", f"{int(prix_m2_med):,} €/m²", delta=delta_prix_m2)
col4.metric("Communes couvertes", f"{nb_communes:,}")

st.divider()

# --- Volume + Prix par année ---
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Volume de transactions par année")
    vol = fct_f.groupby("ANNEE").size().reset_index(name="Transactions")
    fig_vol = px.bar(
        vol, x="ANNEE", y="Transactions",
        labels={"ANNEE": "Année"},
        color_discrete_sequence=["#1f77b4"],
        text="Transactions"
    )
    fig_vol.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
    fig_vol.update_layout(showlegend=False, yaxis_title="Nombre de transactions")
    st.plotly_chart(fig_vol, use_container_width=True)

with col_right:
    st.subheader("Prix/m² médian par année")
    prix_an = fct_f.groupby("ANNEE")["PRIX_M2"].median().reset_index()
    prix_an.columns = ["ANNEE", "Prix_m2_median"]
    fig_prix = px.line(
        prix_an, x="ANNEE", y="Prix_m2_median",
        labels={"ANNEE": "Année", "Prix_m2_median": "Prix/m² médian (€)"},
        markers=True,
        color_discrete_sequence=["#ff7f0e"]
    )
    fig_prix.update_traces(
        text=prix_an["Prix_m2_median"].apply(lambda x: f"{int(x):,} €"),
        textposition="top center",
        mode="lines+markers+text"
    )
    fig_prix.update_layout(yaxis_title="Prix/m² médian (€)")
    st.plotly_chart(fig_prix, use_container_width=True)

st.divider()

# --- Top communes ---
st.subheader("Top 15 communes par prix/m² médian")
st.caption("Communes avec plus de 100 transactions sur la période, prix/m² < 20 000 €")
top_communes = (
    agg_f.groupby("COMMUNE")["PRIX_M2_MEDIAN"]
    .median()
    .reset_index()
    .sort_values("PRIX_M2_MEDIAN", ascending=False)
    .head(15)
)
fig_top = px.bar(
    top_communes,
    x="PRIX_M2_MEDIAN",
    y="COMMUNE",
    orientation="h",
    labels={"PRIX_M2_MEDIAN": "Prix/m² médian (€)", "COMMUNE": ""},
    color="PRIX_M2_MEDIAN",
    color_continuous_scale="Blues",
    text="PRIX_M2_MEDIAN"
)
fig_top.update_traces(texttemplate="%{text:,.0f} €", textposition="outside")
fig_top.update_layout(yaxis=dict(autorange="reversed"), coloraxis_showscale=False)
st.plotly_chart(fig_top, use_container_width=True)

st.divider()

# --- Répartition ---
st.subheader("Maisons vs Appartements")
col_l, col_r = st.columns(2)

with col_l:
    repartition = fct_f["TYPE_LOCAL"].value_counts().reset_index()
    repartition.columns = ["Type", "Transactions"]
    fig_pie = px.pie(
        repartition, values="Transactions", names="Type",
        color_discrete_sequence=["#1f77b4", "#ff7f0e"]
    )
    fig_pie.update_traces(textinfo="percent+label")
    st.plotly_chart(fig_pie, use_container_width=True)

with col_r:
    prix_type = fct_f.groupby("TYPE_LOCAL").agg(
        prix_m2_median=("PRIX_M2", "median"),
        prix_median=("VALEUR_FONCIERE", "median"),
        nb=("VALEUR_FONCIERE", "count")
    ).reset_index()
    prix_type.columns = ["Type", "Prix/m² médian", "Prix médian", "Transactions"]

    fig_compare = px.bar(
        prix_type,
        x="Type",
        y="Prix/m² médian",
        color="Type",
        color_discrete_sequence=["#1f77b4", "#ff7f0e"],
        text="Prix/m² médian"
    )
    fig_compare.update_traces(
        texttemplate="%{text:,.0f} €/m²",
        textposition="outside"
    )
    fig_compare.update_layout(showlegend=False, yaxis_title="Prix/m² médian (€)")
    st.plotly_chart(fig_compare, use_container_width=True)

st.divider()

# --- Tableau départements ---
st.subheader("Classement des départements par prix/m²")
dept_table = (
    fct_f.groupby("CODE_DEPARTEMENT")
    .agg(
        nb_transactions=("VALEUR_FONCIERE", "count"),
        prix_median=("VALEUR_FONCIERE", "median"),
        prix_m2_median=("PRIX_M2", "median"),
        surface_mediane=("SURFACE_REELLE_BATI", "median")
    )
    .reset_index()
    .sort_values("prix_m2_median", ascending=False)
    .head(20)
)
dept_table["prix_median_fmt"] = dept_table["prix_median"].apply(lambda x: f"{int(x):,} €")
dept_table["prix_m2_fmt"] = dept_table["prix_m2_median"].apply(lambda x: f"{int(x):,} €/m²")
dept_table["surface_fmt"] = dept_table["surface_mediane"].apply(lambda x: f"{int(x)} m²" if pd.notna(x) else "N/A")
dept_table["nb_fmt"] = dept_table["nb_transactions"].apply(lambda x: f"{x:,}")

dept_display = dept_table[["CODE_DEPARTEMENT", "nb_fmt", "prix_median_fmt", "prix_m2_fmt", "surface_fmt"]].copy()
dept_display.columns = ["Département", "Transactions", "Prix médian", "Prix/m² médian", "Surface médiane"]
st.dataframe(dept_display, use_container_width=True, hide_index=True)