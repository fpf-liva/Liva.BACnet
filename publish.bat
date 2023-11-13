
@echo off
set Version=
for /F "tokens=1,2" %%a in (ReleaseNotes.md) do ( 
    if NOT defined Version set Version=%%b 
)
echo ====================
echo  Version: %Version%
echo ====================
echo on

dotnet publish Liva.BACnet.Device/Liva.BACnet.Device.csproj -c Release -r win-x64 --self-contained -p:Version=%Version% -p:PublishSingleFile=true 

