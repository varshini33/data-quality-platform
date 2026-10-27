[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$')]
    [string]$BucketName,

    [string]$S3Prefix = "raw/olist/initial",

    [string]$DatasetId = "olistbr/brazilian-ecommerce",

    [switch]$KeepDownloadedFiles
)

$ErrorActionPreference = "Stop"

function Assert-CommandExists {
    param([Parameter(Mandatory = $true)][string]$Name)
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command '$Name' was not found. Install it and reopen PowerShell."
    }
}

Assert-CommandExists -Name "kaggle"
Assert-CommandExists -Name "aws"

Write-Host "`n[1/6] Checking AWS identity..." -ForegroundColor Cyan
aws sts get-caller-identity | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "AWS authentication failed. Run 'aws sts get-caller-identity' and resolve the error."
}

Write-Host "[2/6] Checking Kaggle authentication..." -ForegroundColor Cyan
kaggle datasets files $DatasetId --page-size 1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "Kaggle authentication or dataset access failed. Try 'kaggle auth login' and accept the dataset's terms if prompted."
}

$timestamp = Get-Date -Format "yyyyMMddTHHmmss"
$tempRoot = Join-Path $env:TEMP "olist-s3-ingest-$timestamp"
$downloadDir = Join-Path $tempRoot "download"
$extractDir = Join-Path $tempRoot "extracted"
New-Item -ItemType Directory -Force $downloadDir, $extractDir | Out-Null

try {
    Write-Host "[3/6] Downloading Kaggle dataset '$DatasetId'..." -ForegroundColor Cyan
    kaggle datasets download $DatasetId --path $downloadDir
    if ($LASTEXITCODE -ne 0) {
        throw "Kaggle dataset download failed."
    }

    $zipFiles = @(Get-ChildItem -Path $downloadDir -Filter "*.zip" -File)
    if ($zipFiles.Count -eq 0) {
        throw "No ZIP file was downloaded from Kaggle."
    }

    Write-Host "[4/6] Extracting dataset to temporary storage..." -ForegroundColor Cyan
    foreach ($zip in $zipFiles) {
        Expand-Archive -LiteralPath $zip.FullName -DestinationPath $extractDir -Force
    }

    $csvFiles = @(Get-ChildItem -Path $extractDir -Filter "*.csv" -File -Recurse)
    if ($csvFiles.Count -eq 0) {
        throw "No CSV files found after extracting the dataset."
    }

    Write-Host "Found $($csvFiles.Count) CSV files:" -ForegroundColor Green
    $csvFiles | ForEach-Object {
        Write-Host ("  {0} ({1:N0} bytes)" -f $_.Name, $_.Length)
    }

    $region = (aws configure get region).Trim()
    if (-not $region) {
        throw "AWS CLI region is not configured. Set it with: aws configure set region YOUR_BUCKET_REGION"
    }

    $bucketLocation = aws s3api get-bucket-location --bucket $BucketName --output text
    if ($LASTEXITCODE -ne 0) {
        throw "Could not read bucket location. Check the bucket name and S3 permissions."
    }
    if ($bucketLocation -eq "None" -or $bucketLocation -eq "null") {
        $bucketRegion = "us-east-1"
    } else {
        $bucketRegion = $bucketLocation.Trim()
    }

    if ($bucketRegion -ne $region) {
        Write-Warning "Configured AWS region '$region' differs from bucket region '$bucketRegion'. The AWS CLI may redirect the request; consider running: aws configure set region $bucketRegion"
    }

    $destination = "s3://$BucketName/$($S3Prefix.Trim('/'))/"
    Write-Host "[5/6] Uploading CSV files to $destination ..." -ForegroundColor Cyan

    $manifestEntries = @()
    foreach ($file in $csvFiles) {
        $hash = (Get-FileHash -LiteralPath $file.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
        $target = "$destination$($file.Name)"

        aws s3 cp $file.FullName $target --only-show-errors
        if ($LASTEXITCODE -ne 0) {
            throw "Upload failed for '$($file.Name)'. Manifest was not uploaded."
        }

        $manifestEntries += [PSCustomObject]@{
            file_name = $file.Name
            size_bytes = [long]$file.Length
            sha256 = $hash
            s3_uri = $target
            dataset_id = $DatasetId
            ingestion_timestamp_utc = [DateTime]::UtcNow.ToString("o")
        }
        Write-Host "  Uploaded $($file.Name)" -ForegroundColor Green
    }

    $manifest = [PSCustomObject]@{
        dataset_id = $DatasetId
        bucket = $BucketName
        prefix = $S3Prefix.Trim('/')
        created_at_utc = [DateTime]::UtcNow.ToString("o")
        file_count = $manifestEntries.Count
        files = $manifestEntries
    }

    $manifestPath = Join-Path $tempRoot "manifest-$timestamp.json"
    $manifest | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $manifestPath -Encoding utf8
    $manifestKey = "s3://$BucketName/metadata/ingestion-manifests/olist-$timestamp.json"

    Write-Host "[6/6] Uploading manifest and verifying S3 objects..." -ForegroundColor Cyan
    aws s3 cp $manifestPath $manifestKey --only-show-errors
    if ($LASTEXITCODE -ne 0) {
        throw "CSV uploads completed, but manifest upload failed."
    }

    aws s3 ls $destination --recursive
    if ($LASTEXITCODE -ne 0) {
        throw "Files may have uploaded, but S3 verification failed."
    }

    Write-Host "`nSUCCESS: Uploaded $($csvFiles.Count) CSV files." -ForegroundColor Green
    Write-Host "Data destination: $destination"
    Write-Host "Manifest:         $manifestKey"
    Write-Host "Note: Kaggle's Olist dataset is a static snapshot, not a live data feed."
}
finally {
    if ($KeepDownloadedFiles) {
        Write-Host "Temporary files retained at: $tempRoot"
    } else {
        Remove-Item -LiteralPath $tempRoot -Recurse -Force -ErrorAction SilentlyContinue
    }
}
