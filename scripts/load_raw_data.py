import pandas as pd
import snowflake.connector
from pathlib import Path
from snowflake.connector.pandas_tools import write_pandas

# Config Snowflake (CORRECT)
SNOWFLAKE_CONFIG = {
    'user': 'SIDIBOCOUM',
    'password': 'Excalibur@223700',
    'account': 'ANCOCUF-VK22984',
    'warehouse': 'DVF_WH',
    'database': 'DVF_DB',
    'schema': 'RAW'
}

def load_dvf_files():
    # Connexion Snowflake
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    
    # Trouver tous les fichiers .txt
    data_path = Path('data/raw')
    txt_files = sorted(data_path.glob('*.txt'))
    
    print(f"Found {len(txt_files)} files to load\n")
    
    for file in txt_files:
        print(f"Loading {file.name}...")
        
        try:
            df = pd.read_csv(
                file, 
                sep='|', 
                dtype=str,
                low_memory=False,
                encoding='utf-8'
            )
            
            print(f"  - Read {len(df):,} rows")
            
            # Nettoyer les noms de colonnes
            df.columns = [col.lower().strip().replace(' ', '_') for col in df.columns]
            
            # Nom de table : dvf_2020_s2, dvf_2021, etc.
            table_name = file.stem.lower().replace('-', '_')
            
            # Utiliser le schema
            cursor.execute("USE SCHEMA DVF_DB.RAW")
            
            # Écrire par chunks
            for i in range(0, len(df), 10000):
                chunk = df.iloc[i:i+10000]
                
                success, nchunks, nrows, _ = write_pandas(
                    conn=conn,
                    df=chunk,
                    table_name=table_name.upper(),
                    auto_create_table=True,
                    overwrite=(i == 0)
                )
                
                if i % 50000 == 0 and i > 0:
                    print(f"  - Loaded {i:,} rows...")
            
            print(f"Successfully loaded {len(df):,} rows to {table_name.upper()}\n")
            
        except Exception as e:
            print(f"Error loading {file.name}: {str(e)}\n")
            continue
    
    cursor.close()
    conn.close()
    print("Done")

if __name__ == "__main__":
    load_dvf_files()