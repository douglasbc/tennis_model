#!/bin/bash
cd ~
cd ".wine32/drive_c/Program Files/OnCourt"
env WINEPREFIX="/home/douglas/.wine32" wine-stable OnCourt.exe &
sleep 20
killall -9 OnCourt.exe