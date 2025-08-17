import pandas as pd
import hashlib

# Pliki wejściowe
input_zawodnicy = "./mnt/data/Zawodnicy_1_Liga_2024.csv"
output_zawodnicy = "./mnt/data/zawodnicy_dla_bazy_1.csv"
output_wystepowanie = "./mnt/data/wystepowanie_dla_bazy.csv"

# Parametry regat
rok_regat = 2024
numer_rundy = 1
liga_poziom = "1 Liga"

# Wczytaj dane zawodników
df_zawodnicy = pd.read_csv(input_zawodnicy)


# Funkcja do generowania ID zawodnika
def generate_id_zawodnika(imie, nazwisko, klub):
    id_str = imie[:3].upper() + nazwisko[:3].upper() + klub[:3].upper()
    return int(hashlib.md5(id_str.encode()).hexdigest(), 16) % 100000


# Generowanie tabeli Zawodnicy
zawodnicy = []
for _, row in df_zawodnicy.iterrows():
    zawodnik_pelne = row["Zawodnik"].strip()
    klub = row["Klub"].strip()

    # Rozbijamy imię i nazwisko
    parts = zawodnik_pelne.split()
    if len(parts) >= 2:
        imie = parts[0]
        nazwisko = " ".join(parts[1:])  # Reszta to nazwisko
    else:
        imie = parts[0]
        nazwisko = "Brak"  # W przypadku błędnych danych

    id_zawodnika = generate_id_zawodnika(imie, nazwisko, klub)

    zawodnicy.append({
        "ID_Zawodnika": id_zawodnika,
        "Imię": imie,
        "Nazwisko": nazwisko,
        "Email": "",
        "Pozycja_na_lodce": "",
        "Data_wstapienia_do_PLŻ": "",
        "Numer_licencji": "",
        "Numer_ubezpieczenia": ""
    })


zawodnicy_df = pd.DataFrame(zawodnicy)
zawodnicy_df.to_csv(output_zawodnicy, index=False)

# Generowanie tabeli Występowanie w regatach
wystepowanie = []
for _, row in df_zawodnicy.iterrows():
    zawodnik_pelne = row["Zawodnik"].strip()
    klub = row["Klub"].strip()

    # Rozbijamy imię i nazwisko
    parts = zawodnik_pelne.split()
    if len(parts) >= 2:
        imie = parts[0]
        nazwisko = " ".join(parts[1:])
    else:
        imie = parts[0]
        nazwisko = "Brak"

    id_zawodnika = generate_id_zawodnika(imie, nazwisko,klub)
    skrot_klubu = row["Klub"]

    runda = row["Runda"]

    id_regat = f"{rok_regat}{runda}1"  # ID regat zgodnie z formatem
    wystepowanie.append({
        "ID_wystepowania": f"{id_zawodnika}{runda}",
        "ID_Zawodnika": id_zawodnika,
        "ID_Regat": id_regat,
        "Skrot": skrot_klubu,
        "Trening": ""
    })

# Konwersja do DataFrame i zapis do CSV
zawodnicy_df = pd.DataFrame(zawodnicy)
wystepowanie_df = pd.DataFrame(wystepowanie)

wystepowanie_df.to_csv(output_wystepowanie, index=False)

print("Pliki CSV zostały wygenerowane poprawnie! ✅")
print(f"Zawodnicy: {output_zawodnicy}")
print(f"Występowanie w regatach: {output_wystepowanie}")