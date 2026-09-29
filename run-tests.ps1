$ErrorActionPreference = "Stop"
$RepoDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$OutputDir = Join-Path $RepoDir "out"

if (Test-Path $OutputDir) {
    Remove-Item -Recurse -Force $OutputDir
}
New-Item -ItemType Directory -Path $OutputDir | Out-Null

$Sources = Get-ChildItem -Recurse -Filter *.java `
    (Join-Path $RepoDir "src/main/java"), `
    (Join-Path $RepoDir "src/test/java") | ForEach-Object FullName

javac --release 17 -d $OutputDir $Sources
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

foreach ($Runner in @("StarterTestRunner", "CircuitBreakerTestRunner", "TimeoutTestRunner", "ServiceCircuitBreakerTestRunner")) {
    java -cp $OutputDir "com.example.tokenvalidation.$Runner"
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}
