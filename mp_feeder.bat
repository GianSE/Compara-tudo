@echo off
ECHO ===================================================
ECHO  ATIVANDO O AMBIENTE VIRTUAL (venv)...
ECHO ===================================================

:: 1. Ativa o ambiente virtual.
CALL .\venv\Scripts\activate

ECHO.
ECHO ===================================================
ECHO  INICIANDO O SCRIPT main.py...
ECHO ===================================================

:: 2. Executa o script principal do Python
python main.py

ECHO.
ECHO ===================================================
ECHO  EXECUCAO CONCLUIDA
ECHO ===================================================