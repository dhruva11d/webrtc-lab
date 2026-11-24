# Needs: USB debugging, `adb reverse tcp:8080 tcp:8080` and `adb reverse tcp:3478 tcp:3478` (see README).
$url = (Get-Content "$PSScriptRoot\phone-url.txt" -Raw).Trim()
adb shell "am start -a android.intent.action.VIEW -p com.android.chrome -d '$url'"
