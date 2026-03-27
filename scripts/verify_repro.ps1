param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Args
)

$ErrorActionPreference = "Stop"

if (Get-Command py -ErrorAction SilentlyContinue) {
    py -3 scripts/verify_repro.py @Args
} elseif (Get-Command python -ErrorAction SilentlyContinue) {
    python scripts/verify_repro.py @Args
} else {
    Write-Error "Python not found in PATH"
}
