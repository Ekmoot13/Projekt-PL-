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

if __name__ == "__main__":
    merge_csv_files("*_wyscigi.csv", "all_wyscigi.csv", "ID_wyscigu")
    merge_csv_files("*_regaty.csv", "all_regaty.csv", "ID_Regat")
    merge_csv_files("*_miejsca.csv", "all_miejsca.csv", "ID_miejsca")
