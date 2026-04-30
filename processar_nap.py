# -*- coding: utf-8 -*-
import sys, io
if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
"""
=============================================================
  ACOMPANHAMENTO SAR – Pipeline NAP
  Fonte   : 202604291622_69f25a733572303802c7dd18.csv
             (e qualquer CSV gerado com o mesmo layout)
  Saídas  :
    output/tabela_base.csv
    output/acompanhamento_diario.csv
    output/acompanhamento_semanal.csv
    output/acompanhamento_mensal.csv
    output/acompanhamento_por_cidade.csv
  Encoding: latin-1 | Separador: ;
=============================================================
"""

import os
import sys
import re
import glob
import unicodedata
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=pd.errors.ChainedAssignmentError if hasattr(pd.errors, 'ChainedAssignmentError') else UserWarning)

# ─────────────────────────────────────────────────────────────
# CONFIGURAÇÃO
# ─────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / "output"
LOG_DIR    = BASE_DIR / "logs"

OUTPUT_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "processar_nap.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# MAPEAMENTO EXATO DAS COLUNAS DO ARQUIVO
# ─────────────────────────────────────────────────────────────
# Chave = nome canônico usado internamente
# Valor = nome exato (ou variantes) que pode aparecer no CSV
COL_RENAME = {
    # campo canônico        : nome(s) possíveis no CSV (latin-1 decodificado)
    "cidade"                : ["Cidades", "Cidade"],
    "ticket"                : ["N\u00ba do Ticket", "Numero do Ticket", "Ticket", "N do Ticket"],
    "status"                : ["Status"],
    "motivo_status"         : ["Motivos do Status"],
    "organizacao"           : ["Organiza\u00e7\u00e3o", "Organizacao"],
    "grupo"                 : ["Grupo"],
    "designado"             : ["Designado"],
    "ticket_origem"         : ["Ticket no Sistema de Origem"],
    "notif_atlas"           : ["N\u00famero de Notifica\u00e7\u00e3o Atlas", "Numero de Notificacao Atlas"],
    "tipo_incidente"        : ["Tipo Incidente"],
    "cat_prod_1"            : ["Categoriza\u00e7\u00e3o de Produto 1", "Categorizacao de Produto 1"],
    "cat_prod_2"            : ["Categoriza\u00e7\u00e3o de Produto 2", "Categorizacao de Produto 2"],
    "cat_prod_3"            : ["Categoriza\u00e7\u00e3o de Produto 3", "Categorizacao de Produto 3"],
    "cat_op_1"              : ["Categoriza\u00e7\u00e3o Operacional 1", "Categorizacao Operacional 1"],
    "cat_op_2"              : ["Categoriza\u00e7\u00e3o Operacional 2", "Categorizacao Operacional 2"],
    "cat_op_3"              : ["Categoriza\u00e7\u00e3o Operacional 3", "Categorizacao Operacional 3"],
    "abertura"              : ["Abertura"],
    "previsao"              : ["Previs\u00e3o", "Previsao"],
    "fechamento"            : ["Data Resolu\u00e7\u00e3o", "Data Resolucao", "Data Resolu??o"],
    "cat_res_prod_1"        : ["Categoriza\u00e7\u00e3o Produto da Resolu\u00e7\u00e3o 1"],
    "cat_res_prod_2"        : ["Categoriza\u00e7\u00e3o Produto da Resolu\u00e7\u00e3o 2"],
    "cat_res_prod_3"        : ["Categoriza\u00e7\u00e3o Produto da Resolu\u00e7\u00e3o 3"],
    "cat_causa_1"           : ["Categoriza\u00e7\u00e3o de Causa 1"],
    "cat_causa_2"           : ["Categoriza\u00e7\u00e3o de Causa 2"],
    "cat_causa_3"           : ["Categoriza\u00e7\u00e3o de Causa 3"],
    "cat_resolucao_1"       : ["Categoriza\u00e7\u00e3o de Resolu\u00e7\u00e3o 1"],
    "cat_resolucao_2"       : ["Categoriza\u00e7\u00e3o de Resolu\u00e7\u00e3o 2"],
    "cat_resolucao_3"       : ["Categoriza\u00e7\u00e3o de Resolu\u00e7\u00e3o 3"],
    "notas"                 : ["Notas Resolu\u00e7\u00e3o", "Notas Resolucao"],
}

# Status que significam FECHADO/ENCERRADO
STATUS_FECHADO = {"fechado", "resolvido", "cancelado", "encerrado", "closed", "resolved"}


# ─────────────────────────────────────────────────────────────
# UTILIDADES
# ─────────────────────────────────────────────────────────────

def normalize_text(s: str) -> str:
    nfkd = unicodedata.normalize("NFKD", str(s))
    return nfkd.encode("ASCII", "ignore").decode().lower().strip()


def normalize_city(s) -> str:
    if not isinstance(s, str) or s.strip() == "":
        return "INDEFINIDO"
    s = unicodedata.normalize("NFKC", s)
    # Remove sufixo de estado (ex: RECIFE-PE → RECIFE-PE mantemos mas limpamos espaços)
    s = re.sub(r"\s+", " ", s).strip().upper()
    # Substitui underscore por espaço (TRES_RIOS-RJ → TRES RIOS-RJ)
    s = s.replace("_", " ")
    return s


def pct(closed, opened) -> float:
    return round(closed / opened * 100, 2) if opened else 0.0


# ─────────────────────────────────────────────────────────────
# LEITURA E NORMALIZAÇÃO DO CSV
# ─────────────────────────────────────────────────────────────

def find_csv(base_dir: Path) -> Path:
    """Localiza o CSV na pasta raiz (aceita nome fixo ou padrão)."""
    # Tenta nome fixo primeiro
    fixed = base_dir / "202604291622_69f25a733572303802c7dd18.csv"
    if fixed.exists():
        return fixed
    # Qualquer CSV na pasta raiz
    csvs = list(base_dir.glob("*.csv"))
    if csvs:
        # Prefere o mais recente
        return sorted(csvs, key=lambda p: p.stat().st_mtime, reverse=True)[0]
    return None


def build_rename_map(columns) -> dict:
    """Mapeia nomes reais das colunas para os nomes canônicos."""
    rename = {}
    cols_normalized = {normalize_text(c): c for c in columns}
    for canonical, variants in COL_RENAME.items():
        for v in variants:
            nv = normalize_text(v)
            if nv in cols_normalized:
                rename[cols_normalized[nv]] = canonical
                break
    return rename


def load_csv(path: Path) -> pd.DataFrame:
    log.info(f"Lendo CSV: {path.name}")

    # Tenta encodings possíveis
    for enc in ("latin-1", "cp1252", "utf-8-sig", "utf-8"):
        try:
            df = pd.read_csv(
                path,
                sep=";",
                encoding=enc,
                quotechar='"',
                on_bad_lines="skip",
                dtype=str,
                low_memory=False,
            )
            log.info(f"  Encoding detectado: {enc} | {len(df)} linhas | {len(df.columns)} colunas")
            break
        except Exception as e:
            log.debug(f"  Falha com encoding {enc}: {e}")
            df = None

    if df is None or df.empty:
        log.error("Não foi possível ler o CSV com nenhum encoding suportado.")
        sys.exit(1)

    # Limpa nomes de colunas
    df.columns = [c.strip().strip('"') for c in df.columns]
    log.info(f"  Colunas encontradas: {list(df.columns)}")

    # Renomeia para canônico
    rename_map = build_rename_map(df.columns)
    df = df.rename(columns=rename_map)
    log.info(f"  Mapeamento de colunas: {rename_map}")

    return df


# ─────────────────────────────────────────────────────────────
# LIMPEZA E ENRIQUECIMENTO
# ─────────────────────────────────────────────────────────────

def clean_master(df: pd.DataFrame) -> pd.DataFrame:
    # Normaliza cidade
    if "cidade" in df.columns:
        df["cidade"] = df["cidade"].apply(normalize_city)
    else:
        df["cidade"] = "INDEFINIDO"
        log.warning("Coluna 'cidade' não encontrada – usando INDEFINIDO.")

    # Converte datas
    for col in ("abertura", "fechamento", "previsao"):
        if col in df.columns:
            df[col] = pd.to_datetime(
                df[col].str.strip(), format="%Y-%m-%d %H:%M", errors="coerce"
            )
            nulos = df[col].isna().sum()
            if nulos:
                log.info(f"  Coluna '{col}': {nulos} valores nulos/inválidos.")

    # Se fechamento não existir, cria vazia
    if "fechamento" not in df.columns:
        df["fechamento"] = pd.NaT

    # Flag fechado: considera Data Resolução preenchida OU status em STATUS_FECHADO
    fechado_por_data = df["fechamento"].notna()
    if "status" in df.columns:
        fechado_por_status = df["status"].str.lower().str.strip().isin(STATUS_FECHADO)
        df["_fechado"] = (fechado_por_data | fechado_por_status).astype(int)
    else:
        df["_fechado"] = fechado_por_data.astype(int)

    # Remove linhas sem data de abertura
    n_antes = len(df)
    df = df.dropna(subset=["abertura"]).reset_index(drop=True)
    n_depois = len(df)
    if n_antes != n_depois:
        log.warning(f"  Removidas {n_antes - n_depois} linhas sem data de abertura.")

    # Colunas temporais derivadas
    df["data"]       = df["abertura"].dt.normalize()
    df["ano"]        = df["abertura"].dt.year
    df["mes"]        = df["abertura"].dt.month
    df["semana_iso"] = df["abertura"].dt.isocalendar().week.astype(int)

    log.info(f"  Registros válidos após limpeza: {len(df)}")
    log.info(f"  Período: {df['abertura'].min()} → {df['abertura'].max()}")
    log.info(f"  Cidades únicas: {df['cidade'].nunique()}")

    return df


# ─────────────────────────────────────────────────────────────
# GERADORES DE ACOMPANHAMENTO
# ─────────────────────────────────────────────────────────────

def _indicators(grp: pd.DataFrame) -> pd.Series:
    abertos  = len(grp)
    fechados = int(grp["_fechado"].sum())
    diff     = abertos - fechados
    return pd.Series({
        "abertos"       : int(abertos),
        "fechados"      : int(fechados),
        "diferenca"     : int(diff),
        "pct_fechamento": pct(fechados, abertos),
    })


def gen_base(master: pd.DataFrame) -> pd.DataFrame:
    """Base oficial: abertura × fechamento agrupado por mês."""
    log.info("Gerando tabela_base (mensal)…")
    grp = (
        master
        .groupby(["ano", "mes"], sort=True)
        .apply(_indicators, include_groups=False)
        .reset_index()
    )
    grp["periodo"] = grp.apply(lambda r: f"{int(r.ano)}-{int(r.mes):02d}", axis=1)
    return grp[["periodo", "ano", "mes", "abertos", "fechados", "diferenca", "pct_fechamento"]]


def gen_diario(master: pd.DataFrame) -> pd.DataFrame:
    """Consolidação diária – abertura por dia de abertura, fechamento por dia de resolução."""
    log.info("Gerando acompanhamento_diario…")

    ab = master.groupby("data").size().rename("abertos")

    # Fechamentos agrupados pela data de resolução
    if "fechamento" in master.columns:
        fe_df = master.dropna(subset=["fechamento"]).copy()
        fe_df["data_fech"] = fe_df["fechamento"].dt.normalize()
        fe = fe_df.groupby("data_fech").size().rename("fechados")
        fe.index.name = "data"
    else:
        fe = pd.Series(dtype=int, name="fechados")

    df = pd.DataFrame(ab).join(fe, how="outer").fillna(0).astype({"abertos": int, "fechados": int})
    df.index.name = "data"
    df = df.reset_index()
    df["diferenca"]      = df["abertos"] - df["fechados"]
    df["pct_fechamento"] = df.apply(lambda r: pct(r["fechados"], r["abertos"]), axis=1)
    df = df.sort_values("data").reset_index(drop=True)
    df["data"] = df["data"].dt.strftime("%Y-%m-%d")
    return df


def gen_semanal(master: pd.DataFrame) -> pd.DataFrame:
    """Consolidação semanal (semana ISO)."""
    log.info("Gerando acompanhamento_semanal…")
    grp = (
        master
        .groupby(["ano", "semana_iso"], sort=True)
        .apply(_indicators, include_groups=False)
        .reset_index()
    )
    grp["semana"] = grp.apply(lambda r: f"{int(r.ano)}-W{int(r.semana_iso):02d}", axis=1)
    return grp[["semana", "ano", "semana_iso", "abertos", "fechados", "diferenca", "pct_fechamento"]]


def gen_mensal(master: pd.DataFrame) -> pd.DataFrame:
    """Consolidação mensal com nome do mês."""
    MESES = {
        1:"Janeiro",2:"Fevereiro",3:"Março",4:"Abril",5:"Maio",6:"Junho",
        7:"Julho",8:"Agosto",9:"Setembro",10:"Outubro",11:"Novembro",12:"Dezembro",
    }
    log.info("Gerando acompanhamento_mensal…")
    grp = (
        master
        .groupby(["ano", "mes"], sort=True)
        .apply(_indicators, include_groups=False)
        .reset_index()
    )
    grp["mes_nome"] = grp["mes"].map(MESES)
    grp["periodo"]  = grp.apply(lambda r: f"{int(r.ano)}-{int(r.mes):02d}", axis=1)
    return grp[["periodo", "ano", "mes", "mes_nome", "abertos", "fechados", "diferenca", "pct_fechamento"]]


def gen_por_cidade(master: pd.DataFrame) -> pd.DataFrame:
    """Consolidação por cidade com duplo ranking."""
    log.info("Gerando acompanhamento_por_cidade…")

    grp = (
        master
        .groupby("cidade", sort=False)
        .apply(_indicators, include_groups=False)
        .reset_index()
    )
    grp = grp.rename(columns={"diferenca": "backlog"})

    # Ranking por volume (maior abertura)
    grp = grp.sort_values("abertos", ascending=False).reset_index(drop=True)
    grp.insert(0, "rank_volume", range(1, len(grp) + 1))

    # Ranking por pendência (maior backlog)
    rank_pen = grp.sort_values("backlog", ascending=False)["cidade"].reset_index(drop=True)
    rank_map = {cidade: i + 1 for i, cidade in enumerate(rank_pen)}
    grp["rank_pendencia"] = grp["cidade"].map(rank_map)

    return grp[["rank_volume", "cidade", "abertos", "fechados", "backlog", "pct_fechamento", "rank_pendencia"]]


def gen_por_status(master: pd.DataFrame) -> pd.DataFrame:
    """Distribuição por status."""
    log.info("Gerando acompanhamento_por_status…")
    if "status" not in master.columns:
        return pd.DataFrame()
    grp = master.groupby("status").size().reset_index(name="quantidade")
    grp["pct"] = grp["quantidade"].apply(lambda x: pct(x, len(master)))
    return grp.sort_values("quantidade", ascending=False).reset_index(drop=True)


def gen_por_grupo(master: pd.DataFrame) -> pd.DataFrame:
    """Consolidação por grupo/equipe."""
    log.info("Gerando acompanhamento_por_grupo…")
    if "grupo" not in master.columns:
        return pd.DataFrame()
    grp = (
        master
        .groupby("grupo", sort=False)
        .apply(_indicators, include_groups=False)
        .reset_index()
    )
    grp = grp.rename(columns={"diferenca": "backlog"})
    return grp.sort_values("abertos", ascending=False).reset_index(drop=True)


# ─────────────────────────────────────────────────────────────
# EXPORTAÇÃO
# ─────────────────────────────────────────────────────────────

def export_csv(df: pd.DataFrame, filename: str, label: str):
    if df is None or df.empty:
        log.warning(f"  [!] {label} - sem dados, arquivo nao gerado.")
        return
    path = OUTPUT_DIR / filename
    df.to_csv(path, index=False, encoding="utf-8-sig")
    log.info(f"  [OK] {label} -> {path}  ({len(df)} registros)")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    log.info("=" * 65)
    log.info("  ACOMPANHAMENTO SAR – GPON NAP")
    log.info(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 65)

    # 1. Localizar CSV
    csv_path = find_csv(BASE_DIR)
    if not csv_path:
        log.error(
            "\n  CSV não encontrado!\n"
            f"  Coloque o arquivo CSV exportado na pasta:\n"
            f"  {BASE_DIR}\n"
        )
        sys.exit(1)

    log.info(f"Arquivo selecionado: {csv_path.name}")

    # 2. Carregar e limpar
    raw   = load_csv(csv_path)
    master = clean_master(raw)

    # 3. Gerar consolidações
    tabela_base   = gen_base(master)
    diario        = gen_diario(master)
    semanal       = gen_semanal(master)
    mensal        = gen_mensal(master)
    por_cidade    = gen_por_cidade(master)
    por_status    = gen_por_status(master)
    por_grupo     = gen_por_grupo(master)

    # 4. Exportar CSVs
    log.info("Exportando CSVs…")
    export_csv(tabela_base,  "tabela_base.csv",               "Tabela Base (mensal)")
    export_csv(diario,       "acompanhamento_diario.csv",     "Diário")
    export_csv(semanal,      "acompanhamento_semanal.csv",    "Semanal")
    export_csv(mensal,       "acompanhamento_mensal.csv",     "Mensal")
    export_csv(por_cidade,   "acompanhamento_por_cidade.csv", "Por Cidade")
    export_csv(por_status,   "acompanhamento_por_status.csv", "Por Status")
    export_csv(por_grupo,    "acompanhamento_por_grupo.csv",  "Por Grupo/Equipe")

    # 5. Resumo geral
    total    = len(master)
    fechados = int(master["_fechado"].sum())
    abertos  = total - fechados

    print("\n" + "=" * 65)
    print("  RESUMO GERAL")
    print("=" * 65)
    print(f"  Arquivo processado          : {csv_path.name}")
    print(f"  Total de tickets            : {total:>6}")
    print(f"  Fechados/Resolvidos         : {fechados:>6}")
    print(f"  Em aberto (backlog)         : {abertos:>6}")
    print(f"  % fechamento geral          : {pct(fechados, total):>6.1f}%")
    print(f"  Cidades identificadas       : {master['cidade'].nunique():>6}")

    if "grupo" in master.columns:
        print(f"  Grupos/Equipes              : {master['grupo'].nunique():>6}")
    if "status" in master.columns:
        status_counts = master["status"].value_counts()
        print(f"  Distribuição de status:")
        for s, n in status_counts.items():
            print(f"    {'  ' + str(s):<30}: {n}")

    per_start = master["abertura"].min().strftime("%d/%m/%Y")
    per_end   = master["abertura"].max().strftime("%d/%m/%Y")
    print(f"  Período (abertura)          : {per_start} a {per_end}")
    print("=" * 65)
    print(f"\n  [OK] CSVs disponiveis em: {OUTPUT_DIR}\n")

    log.info("Processamento concluído.")


if __name__ == "__main__":
    main()
