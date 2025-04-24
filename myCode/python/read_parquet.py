import pandas as pd


# Path to your parquet file
parquet_file_path = "parquet/2.parquet"

try:
    df = pd.read_parquet(parquet_file_path)
    print(df)
    df.to_csv('parquet/1.csv', index=False)
except Exception as e:
    print("[bold red]❌ Failed to Read Parquet File")
    print(e)
