@echo off
REM Abre uma janela de terminal para cada réplica (Windows)
cd /d %~dp0
for %%i in (1 2 3) do start "R%%i" cmd /k python replica.py --id %%i
