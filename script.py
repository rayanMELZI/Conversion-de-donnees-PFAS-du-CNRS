import pandas as pd
import ast
import re

# Charger le fichier
df = pd.read_csv('pdh_data.csv', low_memory=False)

def clean_details(row):
    details_str = str(row['details'])
    data = {
        'status': 'Unknown',
        'closure_year': None,
        'pfas_produced': None,
        'last_checked': None
    }
    
    # 1. Essayer de parser le dictionnaire
    try:
        d = ast.literal_eval(details_str)
        data['status'] = d.get('Status', d.get('status', 'Unknown'))
        data['pfas_produced'] = d.get('PFAS produced', None)
        data['last_checked'] = d.get('status last_checked', None)
    except:
        pass
    
    # 2. Extraire l'année de fermeture si présente dans le texte (ex: "Closed in 2014")
    year_match = re.search(r'(?:Closed in|stopped in|stop for|end of)\s+(\d{4})', details_str, re.IGNORECASE)
    if year_match:
        data['closure_year'] = year_match.group(1)
        
    return pd.Series(data)

print("Structuration des données en cours...")

# Appliquer le nettoyage
details_clean = df.apply(clean_details, axis=1)
df = pd.concat([df, details_clean], axis=1)

# Création des labels pour Neo4j
def assign_labels(cat):
    base = "Site"
    if "production facility" in str(cat).lower(): return f"{base}:ProductionFacility"
    if "known pfas user" in str(cat).lower(): return f"{base}:KnownUser"
    if "presumptive" in str(cat).lower(): return f"{base}:PresumptiveSite"
    return base

df['node_labels'] = df['category'].apply(assign_labels)

# ID unique (Nom + Lat si dispo, sinon Nom + Ville)
df['site_id'] = df.apply(lambda r: f"{str(r['name'])}_{r['lat'] if pd.notnull(r['lat']) else r['city']}".replace(" ", "_").lower(), axis=1)

# --- GÉNÉRATION DES 4 FICHIERS ---

# 1. SITES (Colonnes propres)
nodes_sites = df[['site_id', 'node_labels', 'name', 'city', 'country', 'status', 'closure_year', 'pfas_produced', 'last_checked', 'sector', 'lat', 'lon']].drop_duplicates('site_id')
nodes_sites.to_csv('nodes_sites_clean.csv', index=False)

# 2. MESURES (Uniquement pour la catégorie Measurement)
meas_df = df[df['category'] == 'Measurement'].copy()
meas_df['meas_id'] = "meas_" + meas_df.index.astype(str)
nodes_measurements = meas_df[['meas_id', 'site_id', 'date', 'matrix', 'pfas_sum', 'unit']]
nodes_measurements.to_csv('nodes_measurements.csv', index=False)

# 3. SUBSTANCES & 4. RELATIONS (Explosion du JSON)
substances = set()
edges_meas_sub = []
edges_prod_sub = [] # Liens directs Usine -> Molécule

for _, row in df.iterrows():
    # Cas des mesures
    if row['category'] == 'Measurement':
        try:
            pfas_list = ast.literal_eval(row['pfas_values'])
            for p in pfas_list:
                sub = p.get('substance')
                if sub:
                    substances.add(sub)
                    edges_meas_sub.append({'meas_id': f"meas_{_}", 'substance': sub, 'value': p.get('value')})
        except: pass
    
    # Cas des usines qui citent une substance produite
    if pd.notnull(row['pfas_produced']):
        sub = str(row['pfas_produced']).strip()
        substances.add(sub)
        edges_prod_sub.append({'site_id': row['site_id'], 'substance': sub})

pd.DataFrame([{'name': s} for s in substances]).to_csv('nodes_substances.csv', index=False)
pd.DataFrame(edges_meas_sub).to_csv('edges_detections.csv', index=False)
pd.DataFrame(edges_prod_sub).to_csv('edges_production_direct.csv', index=False)

print("Fichiers prêts pour un schéma manuel !")