import pandas as pd
import hashlib
import os
import re
import numpy as np

# ========================================
#  FUNKCJE POMOCNICZE – WSPÓLNE Z kluby.py
# ========================================

def _norm(s: str) -> str:
    """Podstawowa normalizacja – przycięcie i zbicie wielu spacji w jedną."""
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).strip())


def _norm_key(s: str) -> str:
    """
    Normalizacja skrótu klubu:
    - przycięcie,
    - zbicie wielu spacji,
    - USUNIĘCIE wszystkich spacji,
    - wielkie litery.
    Dzięki temu 'PJK', 'P J K', ' PJK ' -> 'PJK'.
    """
    base = _norm(s)
    base = base.replace(" ", "")
    return base.upper()


def _norm_name(s: str) -> str:
    """Normalizacja nazwy klubu – tylko porządkowanie spacji."""
    return _norm(s)


def generate_numeric_id(typ: str, liga_poziom: str, **params) -> str:
    """
    Kanoniczne ID: 8 cyfr z SHA1 (ta sama logika co w kluby.py).
    """
    param_string = f"liga_poziom={liga_poziom}|" + "|".join(
        f"{k}={v}" for k, v in sorted(params.items())
    )
    base_string = f"{typ}|{param_string}"
    hash_bytes = hashlib.sha1(base_string.encode()).digest()
    numeric_id = int.from_bytes(hash_bytes[:4], byteorder="big")
    return f"{numeric_id % 100_000_000:08d}"


def calc_club_variant_id(skrot_raw: str, nazwa_raw: str) -> str:
    """
    Jedyny sposób liczenia ID_wariantu_klubu – identyczny w main.py i kluby.py.
    """
    return generate_numeric_id(
        "KLUB_WARIANT", "ALL",
        Skrot=_norm_key(skrot_raw),
        Nazwa=_norm_name(nazwa_raw),
    )


def parse_miejsce(raw_val, max_miejsce):
    if pd.isna(raw_val):
        return float(max_miejsce), 0

    s = str(raw_val).strip()

    penalty_tags = ["(DNF)", "(SCP)", "(DSQ)", "(OCS)", "(DNC)", "(DNE)", "(RET)", "(TLE)"]
    if any(tag in s for tag in penalty_tags):
        return float(max_miejsce), 1

    specials = {
        "2.5 (RDG)": (2.5, 1),
        "13 (DSQ + SCP)": (13.0, 1),
        "10 (OCS0": (10.0, 1),
    }
    if s in specials:
        return float(specials[s][0]), 1

    m = re.search(r'\d+(\.\d+)?', s)
    if m:
        return float(m.group()), 0

    return float(max_miejsce), 0


def ustal_parametry_z_csv(df: pd.DataFrame):
    race_cols = [col for col in df.columns if re.match(r'^[FR]\d+$', col)]
    race_cols.sort(key=lambda x: int(re.search(r'\d+', x).group()))
    has_final = 'FNL' in df.columns

    miejsca_num = []
    scan_cols = race_cols + (['FNL'] if has_final else [])

    for col in scan_cols:
        for val in df[col]:
            if pd.isna(val):
                continue
            m = re.search(r'\d+(\.\d+)?', str(val))
            if m:
                try:
                    miejsca_num.append(float(m.group()))
                except:
                    pass

    max_miejsce = int(np.nanmax(miejsca_num)) if miejsca_num else 0
    return race_cols, has_final, max_miejsce


def znajdz_kolumne_m_sce(df: pd.DataFrame):
    if "M-sce" in df.columns:
        return "M-sce"

    pattern = re.compile(
        r'(?i)^\s*m\s*[-\.]?\s*ś?\s*ce\s*$|^\s*miejsce( w regatach)?\s*$',
        re.IGNORECASE,
    )

    for col in df.columns:
        if pattern.match(str(col)):
            return col

    raise ValueError("Brak kolumny z miejscem w regatach (np. M-sce).")


# ========================================
#  GŁÓWNY PROGRAM
# ========================================

base_dir = "./mnt/data/Regaty"
output_dir = "./mnt/data/output/main"

os.makedirs(output_dir, exist_ok=True)

for year_folder in os.listdir(base_dir):
    year_path = os.path.join(base_dir, year_folder)
    if not os.path.isdir(year_path):
        continue

    for liga_folder_raw in os.listdir(year_path):
        liga_path = os.path.join(year_path, liga_folder_raw)
        if not os.path.isdir(liga_path):
            continue

        if liga_folder_raw and liga_folder_raw[0].isdigit():
            liga_folder_name = liga_folder_raw[0] + " " + liga_folder_raw[1:]
        else:
            liga_folder_name = liga_folder_raw

        for runda_folder in os.listdir(liga_path):
            runda_path = os.path.join(liga_path, runda_folder)
            if not os.path.isdir(runda_path):
                continue

            nr_match = re.search(r"(\d+)$", runda_folder)
            if not nr_match:
                print(f"⚠ Pomijam folder rundy (brak nr): {runda_folder}")
                continue

            numer_rundy = int(nr_match.group(1))
            rok_regat = int(year_folder)

            for file in os.listdir(runda_path):
                if not file.endswith(".csv"):
                    continue

                try:
                    miasto = file.split("-")[1].replace(".csv", "").strip() if "-" in file else ""

                    input_file = os.path.join(runda_path, file)
                    df = pd.read_csv(input_file)

                    race_cols, has_final, max_miejsce = ustal_parametry_z_csv(df)

                    prefix = f"{output_dir}/{liga_folder_name}_{rok_regat}_{numer_rundy}"
                    os.makedirs(output_dir, exist_ok=True)

                    id_regat = generate_numeric_id(
                        "regaty",
                        liga_folder_name,
                        rok=rok_regat,
                        runda=numer_rundy
                    )

                    regaty = pd.DataFrame([{
                        "ID_Regat": id_regat,
                        "Nazwa": f"{liga_folder_name} - Runda {numer_rundy}",
                        "Liga_Poziom": liga_folder_name,
                        "Miasto": miasto,
                        "Numer_Rundy": numer_rundy,
                        "Rok": rok_regat
                    }])

                    wyscigi = []
                    for col in race_cols:
                        idx = int(re.search(r'\d+', col).group())
                        wyscigi.append({
                            "ID_wyscigu": generate_numeric_id(
                                "wyscig", liga_folder_name,
                                rok=rok_regat, runda=numer_rundy,
                                index=idx, race=col
                            ),
                            "ID_Regat": id_regat,
                            "Numer_wyscigu": idx,
                            "Finalowy": False
                        })

                    if has_final:
                        wyscigi.append({
                            "ID_wyscigu": generate_numeric_id(
                                "wyscig", liga_folder_name,
                                rok=rok_regat, runda=numer_rundy,
                                index=0, race="FNL"
                            ),
                            "ID_Regat": id_regat,
                            "Numer_wyscigu": 0,
                            "Finalowy": True
                        })

                    club_col = "Skrót" if "Skrót" in df.columns else ("Zespół" if "Zespół" in df.columns else None)
                    if club_col is None:
                        raise ValueError("Brak kolumny 'Skrót' lub 'Zespół' w pliku.")

                    miejsca = []
                    for _, row in df.iterrows():
                        skrot = str(row[club_col]).strip()

                        # nazwa klubu z pliku (jeśli jest)
                        if "Klub" in df.columns:
                            nazwa_klubu = row["Klub"]
                        else:
                            nazwa_klubu = ""

                        id_wariantu = calc_club_variant_id(skrot, nazwa_klubu)

                        for col in race_cols:
                            idx = int(re.search(r'\d+', col).group())
                            raw_val = row[col] if col in df.columns else np.nan
                            miejsce_val, kara = parse_miejsce(raw_val, max_miejsce)

                            id_wys = generate_numeric_id(
                                "wyscig", liga_folder_name,
                                rok=rok_regat, runda=numer_rundy,
                                index=idx, race=col
                            )

                            miejsca.append({
                                "ID_miejsca": "",
                                "ID_wyscigu": id_wys,
                                "ID_wariantu_klubu": id_wariantu,
                                "Zajete_miejsce": miejsce_val,
                                "Kary": kara,
                                "Numer_lodki": 0
                            })

                        if has_final:
                            raw_fnl = row["FNL"] if "FNL" in df.columns else np.nan
                            miejsce_fnl, kara_fnl = parse_miejsce(raw_fnl, max_miejsce)

                            id_fnl = generate_numeric_id(
                                "wyscig", liga_folder_name,
                                rok=rok_regat, runda=numer_rundy,
                                index=0, race="FNL"
                            )

                            miejsca.append({
                                "ID_miejsca": "",
                                "ID_wyscigu": id_fnl,
                                "ID_wariantu_klubu": id_wariantu,
                                "Zajete_miejsce": miejsce_fnl,
                                "Kary": kara_fnl,
                                "Numer_lodki": 0
                            })

                    wynik_rows = []
                    try:
                        m_col = znajdz_kolumne_m_sce(df)
                    except:
                        m_col = None

                    if m_col is not None:
                        for _, row in df.iterrows():
                            skrot = str(row[club_col]).strip()

                            if "Klub" in df.columns:
                                nazwa_klubu = row["Klub"]
                            else:
                                nazwa_klubu = ""

                            id_wariantu = calc_club_variant_id(skrot, nazwa_klubu)

                            raw_m = row[m_col]
                            if pd.isna(raw_m):
                                continue

                            m = re.search(r'\d+', str(raw_m))
                            if not m:
                                continue

                            miejsce_w_reg = int(m.group())

                            wynik_rows.append({
                                "ID_wynikRegat": "",
                                "regaty": id_regat,
                                "ID_wariantu_klubu": id_wariantu,
                                "miejsceWRegatach": miejsce_w_reg
                            })

                    pd.DataFrame(miejsca).to_csv(f"{prefix}_miejsca.csv", index=False)
                    pd.DataFrame(wyscigi).to_csv(f"{prefix}_wyscigi.csv", index=False)
                    regaty.to_csv(f"{prefix}_regaty.csv", index=False)

                    if wynik_rows:
                        pd.DataFrame(wynik_rows).to_csv(f"{prefix}_wynikRegat.csv", index=False)

                    print(f"✅ Przetworzono: {file}")

                except Exception as e:
                    print(f"❌ Błąd przy pliku {file}: {e}")
                    continue
