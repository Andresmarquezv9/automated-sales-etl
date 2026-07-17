# =================================================================================================
# LIBRERIAS
# =================================================================================================
import os
import glob
import pandas as pd
import numpy as np

# =================================================================================================
# RUTAS
# =================================================================================================
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
SILVER_DIR = os.path.join(BASE_DIR, "..", "2_silver")
GOLD_DIR   = os.path.join(BASE_DIR, "..", "3_gold")

os.makedirs(GOLD_DIR, exist_ok=True)


pending_files = sorted(
    glob.glob(os.path.join(SILVER_DIR, "Sales_table*.parquet")),
    key=os.path.getmtime, 
)

if not pending_files:
    print("No hay archivos nuevos en 2_silver para procesar. Nada que hacer.")
    raise SystemExit(0)

print(f"Archivos pendientes de cargar a Gold: {len(pending_files)}")
for f in pending_files:
    print(f"   - {os.path.basename(f)}")


# =================================================================================================
# MOTOR DE CARGA INCREMENTAL (UPSERT) - dimensiones
# =================================================================================================
def update_dimension(new_data, filename, business_keys, sk_name):
    """
    Compara la data nueva con la existente. Si hay registros nuevos,
    les crea un nuevo SK continuando la numeración y los anexa.
    """
    filepath = os.path.join(GOLD_DIR, filename)

    if not os.path.exists(filepath):
        new_data = new_data.drop_duplicates(subset=business_keys).reset_index(drop=True)
        new_data.insert(0, sk_name, range(1, len(new_data) + 1))
        new_data.to_parquet(filepath, index=False)
        print(f"   [NUEVA] {filename}: creada con {len(new_data)} registros.")
        return new_data

    existing_dim = pd.read_parquet(filepath)
    merged = new_data.merge(existing_dim[business_keys], on=business_keys, how="left", indicator=True)
    nuevos = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
    nuevos = nuevos.drop_duplicates(subset=business_keys).reset_index(drop=True)

    if len(nuevos) > 0:
        max_sk = existing_dim[sk_name].max()
        nuevos.insert(0, sk_name, range(int(max_sk) + 1, int(max_sk) + 1 + len(nuevos)))
        updated_dim = pd.concat([existing_dim, nuevos], ignore_index=True)
        updated_dim.to_parquet(filepath, index=False)
        print(f"   [ACTUALIZADA] {filename}: +{len(nuevos)} registros nuevos (total: {len(updated_dim)}).")
        return updated_dim

    print(f"   [SIN CAMBIOS] {filename}: no hay registros nuevos.")
    return existing_dim


def append_fact(fact_new: pd.DataFrame, filename: str = "fact_sales.parquet") -> pd.DataFrame:
    """Anexa filas nuevas a la tabla de hechos, continuando la numeración de Fact_SK."""
    filepath = os.path.join(GOLD_DIR, filename)

    if not os.path.exists(filepath):
        fact_new = fact_new.copy()
        fact_new.insert(0, "Fact_SK", range(1, len(fact_new) + 1))
        fact_new.to_parquet(filepath, index=False)
        print(f"   [NUEVA] {filename}: creada con {len(fact_new)} transacciones.")
        return fact_new

    existing_fact = pd.read_parquet(filepath)
    max_sk = existing_fact["Fact_SK"].max()
    fact_new = fact_new.copy()
    fact_new.insert(0, "Fact_SK", range(int(max_sk) + 1, int(max_sk) + 1 + len(fact_new)))
    updated_fact = pd.concat([existing_fact, fact_new], ignore_index=True)
    updated_fact.to_parquet(filepath, index=False)
    print(f"   [ACTUALIZADA] {filename}: +{len(fact_new)} transacciones (total: {len(updated_fact)}).")
    return updated_fact


# =================================================================================================
# FUNCIONES AUXILIARES
# =================================================================================================
def most_frequent(series: pd.Series):
    mode = series.dropna().mode()
    return mode.iloc[0] if not mode.empty else np.nan


def build_calendar(dates: pd.Series) -> pd.DataFrame:
    cal = pd.DataFrame({"Date": pd.Series(dates).dropna().drop_duplicates().sort_values().reset_index(drop=True)})
    cal["Year"]       = cal["Date"].dt.year
    cal["Quarter"]    = cal["Date"].dt.quarter
    cal["Month"]      = cal["Date"].dt.month
    cal["Month_Name"] = cal["Date"].dt.month_name()
    cal["Day"]        = cal["Date"].dt.day
    cal["Week_Day"]   = cal["Date"].dt.day_name()
    return cal


# =================================================================================================
# PROCESAMIENTO: un archivo silver a la vez
# =================================================================================================

for filepath in pending_files:
    print(f"\n=== Procesando: {os.path.basename(filepath)} ===")

    df = pd.read_parquet(filepath)
    n_before = len(df)
    df = df.drop_duplicates().reset_index(drop=True)
    print(f"   Filas: {len(df):,}  (duplicados internos eliminados: {n_before - len(df):,})")

    try:
        # --- Dimensiones ---
        new_items = df.groupby(["Group", "Item_Code"])["Description"].apply(most_frequent).reset_index()
        dim_item = update_dimension(new_items, "dim_item.parquet", ["Group", "Item_Code"], "Item_SK")

        new_customers = df.groupby("Code")["Name"].apply(most_frequent).reset_index()
        new_customers["Name"] = new_customers["Name"].fillna("SIN NOMBRE REGISTRADO")
        dim_customer = update_dimension(new_customers, "dim_customer.parquet", ["Code"], "Customer_SK")

        new_units = df[["Unit"]].dropna().drop_duplicates().reset_index(drop=True)
        dim_unit = update_dimension(new_units, "dim_unit.parquet", ["Unit"], "Unit_SK")

        all_dates = pd.concat([df["Date"], df["Date_2"]], ignore_index=True)
        new_dates = build_calendar(all_dates)
        dim_date = update_dimension(new_dates, "dim_date.parquet", ["Date"], "Date_SK")

        # --- Tabla de hechos ---
        fact = df.merge(dim_item[["Item_SK", "Group", "Item_Code"]], on=["Group", "Item_Code"], how="left")
        fact = fact.merge(dim_customer[["Customer_SK", "Code"]], on="Code", how="left")
        fact = fact.merge(dim_unit[["Unit_SK", "Unit"]], on="Unit", how="left")
        fact = fact.merge(
            dim_date[["Date_SK", "Date"]].rename(columns={"Date_SK": "Invoice_Date_SK"}),
            on="Date", how="left",
        )
        fact = fact.merge(
            dim_date[["Date_SK", "Date"]].rename(columns={"Date_SK": "Reference_Date_SK", "Date": "Date_2"}),
            on="Date_2", how="left",
        )

        fact["Date_Mismatch_Flag"] = (fact["Date"] - fact["Date_2"]).dt.days.abs().gt(30) | fact["Date_2"].isna()
        fact["Value_Mismatch_Flag"] = (fact["Value"] - (fact["Quantity"] * fact["Rate"])).abs().gt(0.05)

        fact_new = fact[[
            "Item_SK", "Customer_SK", "Unit_SK", "Invoice_Date_SK", "Reference_Date_SK",
            "Number", "Reference No.", "Quantity", "Rate", "Value",
            "Date_Mismatch_Flag", "Value_Mismatch_Flag",
        ]].rename(columns={"Number": "Invoice_Number", "Reference No.": "Reference_No"})

        append_fact(fact_new)

    except Exception:
        import traceback
        print(f"    ERROR procesando {os.path.basename(filepath)}.")
        with open(r"C:\Data_lake\Scripts\log_gold.txt", "a") as f:
            f.write(f"\nERROR en {os.path.basename(filepath)}:\n" + traceback.format_exc())
        raise

print("\n Proceso de carga incremental a Gold finalizado con éxito")