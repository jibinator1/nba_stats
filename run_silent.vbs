Set WinScriptHost = CreateObject("WScript.Shell")
' 0 = Hide the window, True = Wait for completion
WinScriptHost.Run "cmd.exe /c ""cd /d ""f:\learning to code\nba_website\nba_stats"" && python daily_update.py > out.txt 2>&1""", 0, True
Set WinScriptHost = Nothing

