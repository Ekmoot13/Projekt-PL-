import pandas as pd
import hashlib
import os
import re
import numpy as np

# Funkcja do generowania deterministycznego ID (8 cyfr)
def generate_numeric_id(typ: str, liga_poziom: str, **params) -> str:
    param_string = f"liga_poziom={liga_poziom}|" + "|".join(f"{k}={v}" for k, v in sorted(params.items()))
    base_string = f"{typ}|{param_string}"
    hash_bytes = hashlib.sha1(base_string.encode()).digest()
    numeric_id = int.from_bytes(hash_bytes[:4], byteorder='big')
    return f"{numeric_id % 100_000_000:08d}"

# (opcjonalnie) konwersja skrótu na liczbowy – nieużywana bezpośrednio do ID
def convert_skrot_to_int(skrot):
    return int(hashlib.md5(skrot.encode()).hexdigest(), 16) % 100000

# Parsowanie miejsca na liczbę + flaga kary
def parse_miejsce(raw_val, max_miejsce):
    if pd.isna(raw_val):
        return float(max_miejsce), 0
    s = str(raw_val).strip()

    # kary powodujące podstawienie max_miejsce
    penalty_tags = ["(DNF)", "(SCP)", "(DSQ)", "(OCS)", "(DNC)", "(DNE)", "(RET)", "(TLE)"]
    if any(tag in s for tag in penalty_tags):
        return float(max_miejsce), 1

    # wyjątki specjalne
    specials = {
        "2.5 (RDG)": (2.5, 1),
        "13 (DSQ + SCP)": (13.0, 1),
        "10 (OCS0": (10.0, 1),  # literówka z danych źródłowych
    }
    if s in specials:
        return float(specials[s][0]), 1

    # ogólne: wyciągnij liczbę (np. "10 (OCS)" -> 10)
    m = re.search(r'\d+(\.\d+)?', s)
    if m:
        try:
            return float(m.group()), 0
        except ValueError:
            return float(max_miejsce), 0

    # brak liczby – podstaw max
    return float(max_miejsce), 0

# Automatyczne ustalanie parametrów z pliku CSV
def ustal_parametry_z_csv(df: pd.DataFrame):
    # kolumny biegów: F1, F2, ... oraz/lub R1, R2, ...
    race_cols = [col for col in df.columns if re.match(r'^[FR]\d+$', col)]
    race_cols.sort(key=lambda x: int(re.search(r'\d+', x).group()))
    has_final = 'FNL' in df.columns

    # max_miejsce – największa liczba w biegach + finał (jeśli jest)
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
                except ValueError:
                    pass
    max_miejsce = int(np.nanmax(miejsca_num)) if miejsca_num else 0
    return race_cols, has_final, max_miejsce

# --- Główna pętla przetwarzania ---
base_dir = "./mnt/data/Regaty"

for year_folder in os.listdir(base_dir):
    year_path = os.path.join(base_dir, year_folder)
    if not os.path.isdir(year_path):
        continue

    for liga_folder_raw in os.listdir(year_path):
        liga_path = os.path.join(year_path, liga_folder_raw)
        if not os.path.isdir(liga_path):
            continue

        # Nazwa ligi do raportów (np. "1Liga" -> "1 Liga")
        if liga_folder_raw and liga_folder_raw[0].isdigit():
            liga_folder_name = liga_folder_raw[0] + " " + liga_folder_raw[1:]
        else:
            liga_folder_name = liga_folder_raw

        for runda_folder in os.listdir(liga_path):
            runda_path = os.path.join(liga_path, runda_folder)
            if not os.path.isdir(runda_path):
                continue

            for file in os.listdir(runda_path):
                if not file.endswith(".csv"):
                    continue

                try:
                    rok_regat = int(year_folder)
                    liga_poziom = liga_folder_name
                    # numer rundy – pobierz końcową liczbę z nazwy folderu
                    nr_match = re.search(r"(\d+)$", runda_folder)
                    if not nr_match:
                        print(f"⚠ Pomijam folder (brak numeru rundy): {runda_folder}")
                        continue
                    numer_rundy = int(nr_match.group(1))

                    # miasto – zakładamy format: "cokolwiek - Miasto.csv"
                    miasto = file.split("-")[1].replace(".csv", "").strip() if "-" in file else ""

                    input_file = os.path.join(runda_path, file)
                    df = pd.read_csv(input_file)

                    race_cols, has_final, max_miejsce = ustal_parametry_z_csv(df)

                    output_prefix = f"./mnt/data/output/{liga_poziom}_{rok_regat}_{numer_rundy}"
                    os.makedirs(os.path.dirname(output_prefix), exist_ok=True)

                    id_regat = generate_numeric_id("regaty", liga_poziom, rok=rok_regat, runda=numer_rundy)
                    regaty = pd.DataFrame([{
                        "ID_Regat": id_regat,
                        "Nazwa": f"{liga_poziom} - Runda {numer_rundy}",
                        "Liga_Poziom": liga_poziom,
                        "Miasto": miasto,
                        "Numer_Rundy": numer_rundy,
                        "Rok": rok_regat
                    }])

                    # --- Wyścigi ---
                    wyscigi = []
                    for col in race_cols:
                        idx_num = int(re.search(r'\d+', col).group())   # numer jako liczba
                        race_code = col                                  # pełny kod do ID (np. "F1" / "R1")
                        wyscigi.append({
                            "ID_wyscigu": generate_numeric_id(
                                "wyscig", liga_poziom, rok=rok_regat, runda=numer_rundy,
                                index=idx_num, race=race_code
                            ),
                            "ID_Regat": id_regat,
                            "Numer_wyscigu": idx_num,   # w CSV – sama liczba
                            "Finalowy": False
                        })

                    if has_final:
                        wyscigi.append({
                            "ID_wyscigu": generate_numeric_id(
                                "wyscig", liga_poziom, rok=rok_regat, runda=numer_rundy,
                                index=0, race="FNL"
                            ),
                            "ID_Regat": id_regat,
                            "Numer_wyscigu": 0,   # finał jako 0 (liczba)
                            "Finalowy": True
                        })

                    # Kolumna z nazwą klubu
                    club_col = "Skrót" if "Skrót" in df.columns else ("Zespół" if "Zespół" in df.columns else None)
                    if club_col is None:
                        raise ValueError("Brak kolumny 'Skrót' lub 'Zespół' w pliku.")

                    # --- Miejsca ---
                    miejsca = []
                    for _, row in df.iterrows():
                        skrot_klubu = str(row[club_col]).strip()

                        # zwykłe biegi
                        for col in race_cols:
                            idx_num = int(re.search(r'\d+', col).group())
                            race_code = col
                            raw_val = row[col] if col in df.columns else np.nan
                            miejsce_val, kara = parse_miejsce(raw_val, max_miejsce)

                            id_wyscigu = generate_numeric_id(
                                "wyscig", liga_poziom, rok=rok_regat, runda=numer_rundy,
                                index=idx_num, race=race_code
                            )
                            id_miejsca = generate_numeric_id(
                                "miejsce", liga_poziom, rok=rok_regat, runda=numer_rundy,
                                index=idx_num, race=race_code, klub=skrot_klubu
                            )

                            miejsca.append({
                                "ID_miejsca": id_miejsca,
                                "ID_wyscigu": id_wyscigu,
                                "Skrot": skrot_klubu,
                                "Zajete_miejsce": miejsce_val,  # tylko liczba
                                "Kary": kara,
                                "Numer_lodki": 0
                            })

                        # finał – tylko jeśli istnieje kolumna FNL
                        if has_final:
                            raw_val_fnl = row["FNL"] if "FNL" in df.columns else np.nan
                            miejsce_fnl, kara_fnl = parse_miejsce(raw_val_fnl, max_miejsce)

                            id_wyscigu_fnl = generate_numeric_id(
                                "wyscig", liga_poziom, rok=rok_regat, runda=numer_rundy,
                                index=0, race="FNL"
                            )
                            id_miejsca_fnl = generate_numeric_id(
                                "miejsce", liga_poziom, rok=rok_regat, runda=numer_rundy,
                                index=0, race="FNL", klub=skrot_klubu
                            )

                            miejsca.append({
                                "ID_miejsca": id_miejsca_fnl,
                                "ID_wyscigu": id_wyscigu_fnl,
                                "Skrot": skrot_klubu,
                                "Zajete_miejsce": miejsce_fnl,  # tylko liczba
                                "Kary": kara_fnl,
                                "Numer_lodki": 0
                            })

                    # Zapis
                    pd.DataFrame(miejsca).to_csv(f"{output_prefix}_miejsca.csv", index=False)
                    pd.DataFrame(wyscigi).to_csv(f"{output_prefix}_wyscigi.csv", index=False)
                    regaty.to_csv(f"{output_prefix}_regaty.csv", index=False)

                    print(f"✅ Przetworzono: {file}")

                except Exception as e:
                    print(f"❌ Błąd przy pliku {file}: {e}")
                    continue
