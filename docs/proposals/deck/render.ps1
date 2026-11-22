# Exports every slide of the deck to PNG with PowerPoint, for visual review.
#   powershell -NoProfile -ExecutionPolicy Bypass -File docs/proposals/deck/render.ps1
param(
    [string]$Deck = (Join-Path $PSScriptRoot "..\event-server-redesign.pptx"),
    [string]$Out = (Join-Path $PSScriptRoot "..\..\..\target\deck-render")
)
$Deck = (Resolve-Path $Deck).Path
New-Item -ItemType Directory -Force $Out | Out-Null
$Out = (Resolve-Path $Out).Path
Get-ChildItem $Out -Filter "slide-*.png" | Remove-Item -Force
$app = New-Object -ComObject PowerPoint.Application
try {
    # msoTrue = -1, msoFalse = 0: open read-only, untitled, without a window.
    $pres = $app.Presentations.Open($Deck, -1, 0, 0)
    try {
        for ($i = 1; $i -le $pres.Slides.Count; $i++) {
            $path = Join-Path $Out ("slide-{0:D2}.png" -f $i)
            $pres.Slides.Item($i).Export($path, "PNG", 1600, 900)
        }
        "exported $($pres.Slides.Count) slides to $Out"
    } finally {
        $pres.Close()
    }
} finally {
    $app.Quit()
}
