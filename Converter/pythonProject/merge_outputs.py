
import pandas as pd
import os
import glob

output_dir = "./mnt/data/output"

def merge_csv_files(pattern, output_file, id_column):
    files = glob.glob(os.path.join(output_dir, pattern))
    if not files:
        print(f"⚠ Brak plików pasujących do wzorca: {pattern}")
        return

    dfs = []
    for file in files:
        try:
            df = pd.read_csv(file)
            dfs.append(df)
        except Exception as e:
            print(f"❌ Błąd przy wczytywaniu {file}: {e}")

    if not dfs:
        print(f"⚠ Nie wczytano żadnych danych dla {pattern}")
        return

    merged_df = pd.concat(dfs, ignore_index=True)
    merged_df.to_csv(os.path.join(output_dir, output_file), index=False)
    print(f"✅ Zapisano połączony plik: {output_file} ({len(merged_df)} rekordów)")

    # Sprawdzanie unikalności ID
    if id_column in merged_df.columns:
        duplicates = merged_df[merged_df.duplicated(subset=[id_column], keep=False)]
        if not duplicates.empty:
            print(f"❌ UWAGA: {len(duplicates)} powtórzonych ID w {output_file} (kolumna {id_column})")
            print(duplicates[[id_column]].drop_duplicates())
        else:
            print(f"✅ Brak duplikatów w {output_file} dla ID: {id_column}")
    else:
        print(f"⚠ Kolumna {id_column} nie istnieje w {output_file}")

def merge_wynik_regat(output_file="all_wynikRegat.csv"):
    """
    Łączy wszystkie pliki wyników regat w output_dir.
    Wspiera dwa układy:
      1) pliki obok innych exportów: *_wynikRegat.csv
      2) osobny folder: output/wynikiRegat/*.csv (nazwy plików dowolne)
    Sprawdza duplikaty po 'ID' (jeśli istnieje), w przeciwnym wypadku po (regaty, klub).
    """
    # Zbierz kandydatów rekursywnie
    files = set(glob.glob(os.path.join(output_dir, "**", "*_wynikRegat.csv"), recursive=True))

    # Dodatkowy katalog, jeśli istnieje
    wyniki_dir = os.path.join(output_dir, "wynikiRegat")
    if os.path.isdir(wyniki_dir):
        files.update(glob.glob(os.path.join(wyniki_dir, "*.csv")))

    files = sorted(files)
    if not files:
        print("⚠ Brak plików wyników regat (szukałem *_wynikRegat.csv oraz ./wynikiRegat/*.csv)")
        return

    dfs = []
    for f in files:
        try:
            df = pd.read_csv(f)
            # Uporządkuj nazwy kolumn, jeśli różnią się wielkością liter
            cols_lower = {c.lower(): c for c in df.columns}
            # Mapowanie oczekiwanych kolumn
            rename_map = {}
            if 'id' in cols_lower:            rename_map[cols_lower['id']] = 'ID'
            if 'regaty' in cols_lower:        rename_map[cols_lower['regaty']] = 'regaty'
            if 'klub' in cols_lower:          rename_map[cols_lower['klub']] = 'klub'
            if 'miejscowregatach' in cols_lower: rename_map[cols_lower['miejscowregatach']] = 'miejsceWRegatach'
            if rename_map:
                df = df.rename(columns=rename_map)

            dfs.append(df[['ID','regaty','klub','miejsceWRegatach']]
                       if set(['ID','regaty','klub','miejsceWRegatach']).issubset(df.columns)
                       else df)
        except Exception as e:
            print(f"❌ Błąd przy wczytywaniu {f}: {e}")

    if not dfs:
        print("⚠ Nie wczytano żadnych danych dla wyników regat")
        return

    merged_df = pd.concat(dfs, ignore_index=True)

    # Normalizacja typów
    if 'miejsceWRegatach' in merged_df.columns:
        merged_df['miejsceWRegatach'] = pd.to_numeric(merged_df['miejsceWRegatach'], errors='coerce').astype('Int64')
    if 'regaty' in merged_df.columns:
        merged_df['regaty'] = pd.to_numeric(merged_df['regaty'], errors='coerce')

    # Zapis
    out_path = os.path.join(output_dir, output_file)
    merged_df.to_csv(out_path, index=False)
    print(f"✅ Zapisano połączony plik: {output_file} ({len(merged_df)} rekordów)")

    # Duplikaty
    if 'ID' in merged_df.columns:
        duplicates = merged_df[merged_df.duplicated(subset=['ID'], keep=False)]
        if not duplicates.empty:
            print(f"❌ UWAGA: {len(duplicates)} powtórzonych ID w {output_file} (kolumna ID)")
            print(duplicates[['ID']].drop_duplicates())
        else:
            print("✅ Brak duplikatów po kolumnie ID w all_wynikRegat.csv")
    elif set(['regaty','klub']).issubset(merged_df.columns):
        duplicates = merged_df[merged_df.duplicated(subset=['regaty','klub'], keep=False)]
        if not duplicates.empty:
            print(f"❌ UWAGA: {len(duplicates)} powtórzeń kombinacji (regaty, klub) w {output_file}")
            print(duplicates[['regaty','klub']].drop_duplicates())
        else:
            print("✅ Brak duplikatów po (regaty, klub) w all_wynikRegat.csv")
    else:
        print("⚠ Nie mogę sprawdzić duplikatów — brak kolumn ID ani (regaty, klub)")

if __name__ == "__main__":
    merge_csv_files("*_wyscigi.csv", "all_wyscigi.csv", "ID_wyscigu")
    merge_csv_files("*_regaty.csv", "all_regaty.csv", "ID_Regat")
    merge_csv_files("*_miejsca.csv", "all_miejsca.csv", "ID_miejsca")
    # Nowe: połącz także wszystkie wyniki regat (obsługuje oba układy ścieżek)
    merge_wynik_regat("all_wynikRegat.csv")
