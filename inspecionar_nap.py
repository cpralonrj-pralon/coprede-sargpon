"""
=============================================================
  DIAGNÓSTICO – nap.xlsx
  Uso: python inspecionar_nap.py
  Exibe estrutura de todas as abas para auxiliar o mapeamento.
=============================================================
"""
import sys
from pathlib import Path
import pandas as pd

XLSX_FILE = Path(__file__).parent / "nap.xlsx"

if not XLSX_FILE.exists():
    print(f"[ERRO] Arquivo não encontrado: {XLSX_FILE}")
    sys.exit(1)

xl = pd.ExcelFile(XLSX_FILE, engine="openpyxl")
print(f"\n{'='*60}")
print(f"  Arquivo: {XLSX_FILE.name}")
print(f"  Abas encontradas: {len(xl.sheet_names)}")
print(f"{'='*60}\n")

for name in xl.sheet_names:
    df = xl.parse(name, header=None, nrows=15)
    print(f"{'─'*60}")
    print(f"  ABA: '{name}'  ({df.shape[0]} linhas × {df.shape[1]} colunas mostradas)")
    print(f"{'─'*60}")
    print(df.to_string(index=True, na_rep="·"))
    print()

print(f"{'='*60}")
print("  Inspecão concluída.")
print(f"{'='*60}\n")
