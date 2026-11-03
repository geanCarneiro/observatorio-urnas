@echo off
setlocal
pushd "%~dp0"

where dotnet >nul 2>nul
if errorlevel 1 (
    echo .NET SDK nao foi encontrado no PATH.
    echo Instale o .NET 10 SDK para abrir o dashboard nativo.
    pause
    popd
    exit /b 1
)

tasklist /FI "IMAGENAME eq ObservatorioUrnasDashboard.exe" /NH 2>nul | find /I "ObservatorioUrnasDashboard.exe" >nul
if not errorlevel 1 (
    echo O dashboard nativo ja esta aberto.
    echo Feche a janela antes de compilar e iniciar outra versao.
    pause
    popd
    exit /b 0
)

set "DOTNET_CLI_HOME=%~dp0.dotnet-cli-home"
set "PROJECT=%~dp0frontend\ObservatorioUrnasDashboard\ObservatorioUrnasDashboard.csproj"
set "APP=%~dp0frontend\ObservatorioUrnasDashboard\bin\Release\net10.0-windows\ObservatorioUrnasDashboard.exe"

dotnet build "%PROJECT%" --configuration Release --verbosity quiet
if errorlevel 1 (
    echo.
    echo Nao foi possivel compilar o dashboard nativo.
    pause
    popd
    exit /b 1
)

popd
start "" "%APP%"
exit /b 0
