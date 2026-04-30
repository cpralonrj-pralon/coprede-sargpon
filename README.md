# Acompanhamento SAR – GPON NAP

Dashboard de acompanhamento de tickets SAR de implantação GPON NAP.
Publicado via **GitHub Pages** — acesso direto pelo navegador, sem backend.

---

## 🌐 Acessar o Dashboard

> `https://<seu-usuario>.github.io/<nome-do-repositorio>/`

---

## 🔄 Como atualizar os dados

### 1. Exporte o CSV do sistema
Exporte o relatório normalmente pelo sistema de origem (formato CSV, separador `;`, encoding `latin-1`).

### 2. Substitua o arquivo de dados
Renomeie o arquivo exportado para `nap.csv` e coloque na pasta `data/`:
```
data/
└── nap.csv   ← substitua por aqui
```

### 3. Faça push no GitHub
```bash
git add data/nap.csv
git commit -m "Atualização: nap.csv $(date +%Y-%m-%d)"
git push
```

O dashboard atualiza automaticamente em ~1 minuto após o push.

---

## 📁 Estrutura do projeto

```
/
├── index.html          → dashboard principal (GitHub Pages serve este)
├── dashboard.html      → alias do dashboard
├── processor.js        → lógica de processamento do CSV (JS)
├── data/
│   └── nap.csv         → arquivo de dados atual ← ATUALIZAR AQUI
├── processar_nap.py    → script Python (gera CSVs locais, opcional)
├── executar.bat        → atalho Windows para o script Python
├── .gitignore
└── README.md
```

---

## ⚙️ Funcionamento

O dashboard lê `data/nap.csv` automaticamente ao abrir a página.
Caso queira testar com outro arquivo sem fazer push, use o botão **"Importar CSV"** no canto superior direito.

---

## 🚀 Publicar no GitHub Pages (primeira vez)

1. Crie um repositório no GitHub (público ou privado com Pages habilitado)
2. Execute os comandos abaixo na pasta do projeto:
```bash
git init
git add .
git commit -m "Publicação inicial"
git branch -M main
git remote add origin https://github.com/<usuario>/<repositorio>.git
git push -u origin main
```
3. No GitHub: **Settings → Pages → Source → main → / (root)** → Save
4. Aguarde ~1 minuto e acesse a URL fornecida pelo GitHub Pages
