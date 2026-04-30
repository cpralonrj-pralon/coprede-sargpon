@echo off
chcp 65001 >nul
title Acompanhamento SAR – NAP

echo ============================================================
echo   ACOMPANHAMENTO SAR – Processamento NAP
echo ============================================================
echo.

cd /d "%~dp0"

:: Verifica se o arquivo nap.xlsx existe
if not exist "nap.xlsx" (
    echo [ERRO] Arquivo nap.xlsx nao encontrado nesta pasta!
    echo.
    echo Coloque o arquivo nap.xlsx em:
    echo %~dp0
    echo.
    pause
    exit /b 1
)

echo Iniciando processamento...
echo.

python processar_nap.py

echo.
if %errorlevel% equ 0 (
    echo ============================================================
    echo   Concluido com sucesso! Verifique a pasta OUTPUT\
    echo ============================================================
    echo.
    :: Abre a pasta de saida no Explorer
    explorer output
) else (
    echo [ERRO] O processamento falhou. Verifique os logs em logs\processar_nap.log
)

pause
