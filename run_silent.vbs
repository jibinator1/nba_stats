Set WinScriptHost = CreateObject("WScript.Shell")
' 0 = Hide the window, True = Wait for completion
WinScriptHost.Run "cmd.exe /c python ""f:\learning to code\nba_website\nba_stats\daily_update.py"" > ""f:\learning to code\nba_website\nba_stats\out.txt"" 2>&1", 0, True
Set WinScriptHost = Nothing
