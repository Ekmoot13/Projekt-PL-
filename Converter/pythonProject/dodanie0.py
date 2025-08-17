import pandas as pd
import hashlib
import os
import re

# Funkcja do generowania deterministycznego ID
def generate_numeric_id(typ: str, liga_poziom: str, **params) -> str:
    param_string = f"liga_poziom={liga_poziom}|" + "|".join(f"{k}={v}" for k, v in sorted(params.items()))
    base_string = f"{typ}|{param_string}"
    hash_bytes = hashlib.sha1(base_string.encode()).digest()
    numeric_id = int.from_bytes(hash_bytes[:4], byteorder='big')
    return f"{numeric_id % 100_000_000:08d}"

# Funkcja do konwersji skrótu klubu na int
def convert_skrot_to_int(skrot):
    return int(hashlib.md5(skrot.encode()).hexdigest(), 16) % 100000

def zapytaj_o_parametry_dodatkowe():
    while True:
        try:
            ilosc = int(input("\n➡️ Podaj ilość wyścigów: "))
            break
        except ValueError:
            print("❌ Błąd: podaj liczbę całkowitą.")

    while True:
        final = input("➡️ Czy wyścig finałowy? (t/n): ").strip().lower()
        if final in ['t', 'n']:
            finalowy = 1 if final == 't' else 0
            break
        print("❌ Błąd: wpisz 't' (tak) lub 'n' (nie).")

    while True:
        try:
            max_miejsce = int(input("➡️ Podaj maksymalne miejsce (dla karnych): "))
            break
        except ValueError:
            print("❌ Błąd: podaj liczbę całkowitą.")

    return ilosc, finalowy, max_miejsce


# Przejście przez wszystkie foldery
base_dir = "./mnt/data/Regaty"

for year_folder in os.listdir(base_dir):
    year_path = os.path.join(base_dir, year_folder)
    if not os.path.isdir(year_path):
        continue

    for liga_folder in os.listdir(year_path):
        if liga_folder and liga_folder[0].isdigit():
            liga_folder = liga_folder[0] + " " + liga_folder[1:]

        liga_path = os.path.join(year_path, liga_folder)
        if not os.path.isdir(liga_path):
            continue

        for runda_folder in os.listdir(liga_path):
            runda_path = os.path.join(liga_path, runda_folder)
            if not os.path.isdir(runda_path):
                continue

            for file in os.listdir(runda_path):
                if not file.endswith(".csv"):
                    continue

                try:
                    # Pobieranie danych z nazw
                    rok_regat = int(year_folder)
                    liga_poziom = liga_folder
                    numer_rundy = int(re.search(r"(\d+)$", runda_folder).group(1))
                    miasto = file.split("-")[1].replace(".csv", "").strip()

                    ilosc_wyścigów = int(input(f"Podaj ilość wyścigów dla pliku {file}: "))
                    finalowy = int(input(f"Czy ma być finałowy wyścig? (0/1) dla pliku {file}: "))
                    max_miejsce = int(input(f"Podaj maksymalne miejsce dla pliku {file}: "))

                    if not finalowy:
                        ilosc_wyścigów += 1

                    input_file = os.path.join(runda_path, file)
                    output_prefix = f"./mnt/data/output/{liga_poziom}_{rok_regat}_{numer_rundy}"

                    id_regat = generate_numeric_id("regaty", liga_poziom, rok=rok_regat, runda=numer_rundy)

                    regaty = pd.DataFrame([{"ID_Regat": id_regat,"Nazwa": f"{liga_poziom} - Runda {numer_rundy}","Liga_Poziom": liga_poziom,"Miasto": miasto,"Numer_Rundy": numer_rundy,"Rok": rok_regat}])

                    wyscigi = []
                    for i in range(1, ilosc_wyścigów):
                        wyscigi.append({"ID_wyscigu": generate_numeric_id("wyscig", liga_poziom, rok=rok_regat, runda=numer_rundy, index=i+1),"ID_Regat": id_regat,"Numer_wyscigu": f"F{i}","Finalowy": False})

                    if finalowy:
                        wyscigi.append({"ID_wyscigu": generate_numeric_id("wyscig", liga_poziom, rok=rok_regat, runda=numer_rundy, index=0),"ID_Regat": id_regat,"Numer_wyscigu": "FNL","Finalowy": True})

                    df = pd.read_csv(input_file)
                    miejsca = []

                    for _, row in df.iterrows():
                        skrot_klubu = str(row["Skrót"]).strip()

                        for i in range(1, ilosc_wyścigów):
                            miejsce = str(row[f"F{i}"]).strip()
                            kara = 0

                            try:
                                if any(tag in miejsce for tag in
                                       ["(DNF)", "(SCP)", "(DSQ)", "(OCS)", "(DNC)", "(DNE)", "(RET)", "(TLE)"]):
                                    miejsce = max_miejsce
                                    kara = 1
                                elif miejsce == "2.5 (RDG)":
                                    miejsce = 2.5
                                    kara = 1
                                elif miejsce == "13 (DSQ + SCP)":
                                    miejsce = 13
                                    kara = 1
                                elif miejsce == "10 (OCS0":
                                    miejsce = 10
                                    kara = 1
                                else:
                                    miejsce = float(miejsce)
                            except:
                                miejsce = int(input(f"Podaj wartość miejsca dla {miejsce}: "))

                            id_wyscigu = generate_numeric_id("wyscig", liga_poziom, rok=rok_regat, runda=numer_rundy, index=i+1)
                            id_miejsca = generate_numeric_id("miejsce", liga_poziom, rok=rok_regat, runda=numer_rundy, index=i+1, klub=skrot_klubu)

                            miejsca.append({"ID_miejsca": id_miejsca,"ID_wyscigu": id_wyscigu,"Skrot": skrot_klubu,"Zajete_miejsce": miejsce,"Kary": kara,"Numer_lodki": 0})

                        if finalowy:
                            miejsce_fnl = str(row["FNL"]).strip() if not pd.isna(row["FNL"]) else "100"
                            kara_fnl = 0

                            try:
                                if any(tag in miejsce_fnl for tag in ["(DNF)", "(SCP)", "(DSQ)", "(OCS)"]):
                                    miejsce_fnl = 10
                                    kara_fnl = 1
                                else:
                                    miejsce_fnl = int(float(miejsce_fnl))
                            except:
                                miejsce_fnl = int(input(f"Podaj wartość miejsca dla {miejsce_fnl}: "))


                            id_wyscigu_fnl = generate_numeric_id("wyscig", liga_poziom, rok=rok_regat, runda=numer_rundy, index=0)
                            id_miejsca_fnl = generate_numeric_id("miejsce", liga_poziom, rok=rok_regat, runda=numer_rundy, index=0, klub=skrot_klubu)

                            miejsca.append({"ID_miejsca": id_miejsca_fnl,"ID_wyscigu": id_wyscigu_fnl,"Skrot": skrot_klubu,"Zajete_miejsce": miejsce_fnl,"Kary": kara_fnl,"Numer_lodki": 0})

                    miejsca_df = pd.DataFrame(miejsca)
                    wyscigi_df = pd.DataFrame(wyscigi)

                    miejsca_df.to_csv(f"{output_prefix}_miejsca.csv", index=False)
                    wyscigi_df.to_csv(f"{output_prefix}_wyscigi.csv", index=False)
                    regaty.to_csv(f"{output_prefix}_regaty.csv", index=False)

                    print(f"✅ Przetworzono: {file}")

                except Exception as e:
                    print(f"❌ Błąd przy pliku {file}: {e}")
                    continue