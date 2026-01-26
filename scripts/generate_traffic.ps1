# Traffic Generator for CSC5355 Pipeline
# Generates requests with the specified distribution

param(
    [int]$TotalRequests = 1000,
    [int]$DelayMs = 50
)

Write-Host "Generating $TotalRequests requests with $DelayMs ms delay..."

for ($i = 1; $i -le $TotalRequests; $i++) {
    $rand = Get-Random -Minimum 1 -Maximum 101
    
    if ($rand -le 50) { $product = 1 }      # 50%
    elseif ($rand -le 75) { $product = 2 }  # 25%
    elseif ($rand -le 90) { $product = 3 }  # 15%
    elseif ($rand -le 98) { $product = 4 }  # 8%
    else { $product = 5 }                   # 2%
    
    curl.exe -s "http://localhost:8080/product$product" | Out-Null
    
    if ($i % 100 -eq 0) { 
        Write-Host "Progress: $i/$TotalRequests requests sent" 
    }
    
    Start-Sleep -Milliseconds $DelayMs
}

Write-Host "`n? Complete! Sent $TotalRequests requests."
Write-Host "`nCheck results:"
Write-Host "  docker compose exec cassandra cqlsh -e 'SELECT COUNT(*) FROM csc5355.LOG;'"
