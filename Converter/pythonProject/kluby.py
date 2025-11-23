import os
import re
import glob
import hashlib
import pandas as pd
from collections import Counter

BASE_DIR = "./mnt/data/Regaty"
OUT_DIR  = "./mnt/data/kluby"
MAPPING_PATH = "./mnt/data/kluby/Kluby_tablica.csv"

os.makedirs(OUT_DIR, exist_ok=True)

# ----------------------------------------
# NORMALIZACJA – IDENTYCZNA Z main.py
# ----------------------------------------

def _norm(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    return re.sub(r"\s+", " ", s)


def _norm_key(s: str) -> str:
    """
    Normalizacja skrótu:
    - przycięcie,
    - redukcja wielu spacji,
    - usunięcie spacji ze środka,
    - zamiana na UPPER

    Dzięki temu:
    'P JK', 'P  J K', ' pjk ' -> 'PJK'
    """
    base = _norm(s)
    base = base.replace(" ", "")
    return base.upper()


def _norm_name(s: str) -> str:
    """normalizacja nazwy — pojedyncze spacje (bez zmiany wielkości liter)"""
    return _norm(s)


# ----------------------------------------
# GENEROWANIE ID – WSPÓLNE Z main.py
# ----------------------------------------

def generate_numeric_id(typ: str, liga_poziom: str, **params) -> int:
    param_string = (
        f"liga_poziom={liga_poziom}|"
        + "|".join(f"{k}={v}" for k, v in sorted(params.items()))
    )
    base_string = f"{typ}|{param_string}"
    hash_bytes = hashlib.sha1(base_string.encode()).digest()
    return int.from_bytes(hash_bytes[:4], "big") % 100_000_000


def calc_club_variant_id(skrot_raw: str, nazwa_raw: str) -> int:
    sk = _norm_key(skrot_raw)
    nm = _norm_name(nazwa_raw)
    return generate_numeric_id(
        "KLUB_WARIANT", "ALL", Skrot=sk, Nazwa=nm
    )


# ----------------------------------------
# Wczytywanie klubów z CSV – TAK SAMO JAK W main.py
# ----------------------------------------

def scan_clubs():
    """
    Przechodzi po katalogu ./mnt/data/Regaty TAK SAMO jak main.py i
    z każdego pliku .csv wyciąga pary (Skrot, NazwaKlubu), używając:
      - skrót: kolumna 'Skrót' lub 'Zespół'
      - nazwa: kolumna 'Klub' (jeśli istnieje), inaczej pusty string
    """
    raw_rows = []
    seen_files = 0

    for year_folder in os.listdir(BASE_DIR):
        year_path = os.path.join(BASE_DIR, year_folder)
        if not os.path.isdir(year_path):
            continue

        for liga_folder_raw in os.listdir(year_path):
            liga_path = os.path.join(year_path, liga_folder_raw)
            if not os.path.isdir(liga_path):
                continue

            for runda_folder in os.listdir(liga_path):
                runda_path = os.path.join(liga_path, runda_folder)
                if not os.path.isdir(runda_path):
                    continue

                for file in os.listdir(runda_path):
                    if not file.endswith(".csv"):
                        continue

                    csv_path = os.path.join(runda_path, file)
                    try:
                        df = pd.read_csv(csv_path)
                    except Exception:
                        continue

                    seen_files += 1

                    # dokładnie jak w main.py:
                    club_col = "Skrót" if "Skrót" in df.columns else (
                        "Zespół" if "Zespół" in df.columns else None
                    )
                    if club_col is None:
                        # ten plik main.py też by pominął
                        continue

                    for _, r in df.iterrows():
                        skrot = _norm(r[club_col]) if club_col in df.columns else ""
                        # nazwa klubu – jeśli kolumna istnieje
                        if "Klub" in df.columns:
                            nazwa = _norm(r["Klub"])
                        else:
                            nazwa = ""

                        if not skrot and nazwa:
                            auto = re.sub(r"[^A-Za-z0-9]", "", nazwa.upper())[:6]
                            skrot = auto or "TMP"

                        if not skrot and not nazwa:
                            continue

                        raw_rows.append({"Skrot": skrot, "Nazwa": nazwa})

    return pd.DataFrame(raw_rows), seen_files


# ----------------------------------------
# Unikalne (Skrot, Nazwa)
# ----------------------------------------

def build_pairs(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df.empty:
        return pd.DataFrame(columns=["Skrot", "Nazwa"])

    raw_df["Sk_norm"] = raw_df["Skrot"].map(_norm_key)
    raw_df["Nm_norm"] = raw_df["Nazwa"].map(_norm_name)

    grouped = raw_df.groupby(["Sk_norm", "Nm_norm"]).size().reset_index()

    rows = []
    for _, r in grouped.iterrows():
        sk_norm = r["Sk_norm"]
        nm_norm = r["Nm_norm"]

        # wybieramy najczęściej występujący oryginalny skrót
        forms = raw_df[raw_df["Sk_norm"] == sk_norm]["Skrot"].tolist()
        counter = Counter(forms)
        max_count = max(counter.values())
        best = max([f for f, c in counter.items() if c == max_count], key=len)

        rows.append({
            "Skrot": best,
            "Nazwa": nm_norm
        })

    return pd.DataFrame(rows).sort_values(["Skrot", "Nazwa"])


# ----------------------------------------
# Tablica zestawień klubów
# ----------------------------------------

def load_mapping():
    if not os.path.exists(MAPPING_PATH):
        return pd.DataFrame()

    df = pd.read_csv(MAPPING_PATH)

    rows = []
    for _, r in df.iterrows():
        id_z = r["ID_zestawienia_klubow"]
        sk_field = _norm(r["Skrot"])

        for part in sk_field.split(";"):
            part = _norm(part)
            rows.append({
                "ID_zestawienia_klubow": id_z,
                "Sk_norm": _norm_key(part)
            })

    return pd.DataFrame(rows)


def attach_zestawienie_ids(df_pairs: pd.DataFrame, mapping: pd.DataFrame):
    df = df_pairs.copy()
    df["Sk_norm"] = df["Skrot"].map(_norm_key)
    return df.merge(mapping, on="Sk_norm", how="left").drop(columns=["Sk_norm"])


# ----------------------------------------
# Generowanie ID i zapisywanie
# ----------------------------------------

def add_variant_ids(df: pd.DataFrame):
    df = df.copy()
    df["ID_wariantu_klubu"] = df.apply(
        lambda r: calc_club_variant_id(r["Skrot"], r["Nazwa"]),
        axis=1
    )
    return df


def save_outputs(df):
    out_csv = os.path.join(OUT_DIR, "kluby_wyciag.csv")
    out_sql = os.path.join(OUT_DIR, "kluby_insert.sql")

    df.to_csv(out_csv, index=False)

    with open(out_sql, "w", encoding="utf-8") as f:
        for _, r in df.iterrows():
            f.write(
                f"INSERT INTO liga_KlubWariant (ID_wariantu_klubu, Skrot, Nazwa, ID_zestawienia_klubow) "
                f"VALUES ({r['ID_wariantu_klubu']}, '{r['Skrot']}', '{r['Nazwa']}', {int(r['ID_zestawienia_klubow'])});\n"
            )

    print("Zapisano:", out_csv)
    print("Zapisano:", out_sql)


# ----------------------------------------
# MAIN
# ----------------------------------------

def main():
    raw, seen = scan_clubs()
    print(f"Przeskanowano plików: {seen}, zebrano wierszy: {len(raw)}")

    pairs = build_pairs(raw)
    mapping = load_mapping()
    pairs = attach_zestawienie_ids(pairs, mapping)
    final = add_variant_ids(pairs)
    save_outputs(final)

    print("Wierszy po unikalizacji:", len(final))


if __name__ == "__main__":
    main()
