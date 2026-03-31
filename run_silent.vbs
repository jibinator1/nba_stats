Set WinScriptHost = CreateObject("WScript.Shell")
' 0 = Hide the window, True = Wait for completion
WinScriptHost.Run "python ""f:\learning to code\nba_website\nba_stats\daily_update.py""", 0, True
Set WinScriptHost = Nothing
