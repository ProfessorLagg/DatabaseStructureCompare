Param(
    [string]$srcConString = $((Import-Clixml -Path ".\constr.cli.xml").src),
    [string]$dstConString = $((Import-Clixml -Path ".\constr.cli.xml").dst)
)

# === Functions ===
function Invoke-Sql() {
    Param(
        [Parameter(Mandatory = $true, ValueFromPipeline = $true,
            HelpMessage = "Enter a valid TSQL query string")]
        [Alias("Q", "Query")]
        [ValidateNotNullOrEmpty()]
        [string]$SqlQuery,

        [Parameter(Mandatory = $true)]
        [ValidateNotNullOrEmpty()]
        [string]$ConnectionString
    )
    # Load Depedency
    Add-Type -AssemblyName System.Data 
    
    # Open DB Connection
    $Connection = New-Object System.Data.SQLClient.SQLConnection
    $Connection.ConnectionString = $ConnectionString
    $Connection.Open()
    # Create DB command
    $Command = $Connection.CreateCommand()
    $Command.CommandText = $sqlquery
    # Execute Command
    $DataTable = New-Object "System.Data.Datatable"
    $DataReader = $Command.ExecuteReader()
    $DataTable.Load($DataReader)   
    
    # Clean up objects
    $Connection.Close()
    $Connection.Dispose()
    $Command.Dispose()
    $DataReader.Dispose()
    
	[System.GC]::Collect([System.GC]::MaxGeneration, [System.GCCollectionMode]::Forced, $true, $false)
	
    return $DataTable
}
function Clean-SQLString() {
    Param(
        [string]$SqlString
    )

    [string]$result = $SqlString.Trim()

    [Regex]$rgxLineComment = [Regex]::new('--[^\n]*?\n')
    [Regex]$rgxInlineComment = [Regex]::new('\/\*.*?\*\/')

    $result = $rgxLineComment.Replace($result, '')
    $result = $rgxInlineComment.Replace($result, '')

    return $result
}
function Minimize-SQLString() {
    Param(
        [string]$SqlString
    )

    [string]$result = Clean-SQLString -SqlString $SqlString
    
    # Replace all string contents with tokens
    $stringTokens = @{}
    $stringPattern = "'.*?'"
    $stringMatches = [Regex]::Matches($result, $stringPattern) | Sort-Object -Property Value -Unique
    [int]$tokenId = 1
    foreach ($match in $stringMatches) {
        $token = "�$tokenId"
        $stringTokens[$token] = $match.Value
        $result = $result.Replace($match.Value, $token)
        $tokenId++
    }
    # Replace all \s with spaces
    $result = [Regex]::Replace($result, '\s', ' ')
    # Repalce all double spaces with single spaces, until no double spaces exists
    while ($result.Contains('  ')) {
        $result = $result.Replace('  ', ' ')
    }
    # Replace all instances of <space>GO<space> with <newline>GO<newline>
    $result = $result.Replace(' GO ', "`nGO`n")
    # Replace all string tokens with their original content
    foreach ($token in $stringTokens.Keys) {
        $result = $result.Replace($token, $stringTokens[$token])
    }

    return $result
}
# === Enums ===
enum DifferenceType{
    Identical
    NotFoundInSource
    NotFoindInDestination
    DataDiffers
}
# === SCRIPT ===
cd $PSScriptRoot
$ErrorActionPreference = 'Stop'

# --- Table Columns ---
$queryTableColumns = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS as c WHERE ( SELECT TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES as t WHERE c.TABLE_CATALOG = t.TABLE_CATALOG AND c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME ) = 'BASE TABLE'"
$srcTableColumnsResult = @(Invoke-Sql -SqlQuery $queryTableColumns -ConnectionString $srcConString)
$dstTableColumnsResult = @(Invoke-Sql -SqlQuery $queryTableColumns -ConnectionString $dstConString)
$srcTableColumns = @{} 
$srcTableColumnsResult `
| Select-Object -Property TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, NUMERIC_PRECISION, NUMERIC_PRECISION_RADIX, NUMERIC_SCALE, DATETIME_PRECISION, CHARACTER_SET_CATALOG, CHARACTER_SET_SCHEMA, CHARACTER_SET_NAME, COLLATION_CATALOG, COLLATION_SCHEMA, COLLATION_NAME, DOMAIN_CATALOG, DOMAIN_SCHEMA, DOMAIN_NAME `
| ForEach-Object { $srcTableColumns["[$($_.TABLE_SCHEMA)].[$($_.TABLE_NAME)].[$($_.COLUMN_NAME)]"] = $_ }
$dstTableColumns = @{} 
$dstTableColumnsResult `
| Select-Object -Property TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, NUMERIC_PRECISION, NUMERIC_PRECISION_RADIX, NUMERIC_SCALE, DATETIME_PRECISION, CHARACTER_SET_CATALOG, CHARACTER_SET_SCHEMA, CHARACTER_SET_NAME, COLLATION_CATALOG, COLLATION_SCHEMA, COLLATION_NAME, DOMAIN_CATALOG, DOMAIN_SCHEMA, DOMAIN_NAME `
| ForEach-Object { $dstTableColumns["[$($_.TABLE_SCHEMA)].[$($_.TABLE_NAME)].[$($_.COLUMN_NAME)]"] = $_ }
Remove-Variable 'srcTableColumnsResult'
Remove-Variable 'dstTableColumnsResult'

# finding table column differences
$colPropertyNames = $(
    @($srcTableColumns.Values | Get-Member -MemberType NoteProperty) +
    @($dstTableColumns.Values | Get-Member -MemberType NoteProperty)
) | Select-Object -ExpandProperty Name | Sort-Object -Unique
$allColNames = $(@($srcTableColumns.Keys) + @($dstTableColumns.Keys)) | Sort-Object -Unique
$compareTableColumns = @{}

[double]$cnt_cur = 0.0
[double]$cnt_tot = $allColNames.Count
foreach ($k in $allColNames) {
    [double]$prog = [Math]::Min(100.0, $cnt_cur / $cnt_tot)
    [string]$stat = "$($prog.ToString('p')) | $($cnt_cur.ToString('#,##0')) / $($cnt_tot.ToString('#,##0'))"
    Write-Progress -Id 1 -Activity "Comparing table definitions" -Status $stat -CurrentOperation $k -PercentComplete $($prog * 100.0)

    $srcCol = $srcTableColumns[$k]
    $dstCol = $dstTableColumns[$k]
    [bool]$srcFound = $null -ne $srcCol
    [bool]$dstFound = $null -ne $dstCol
    [bool]$dataSame = $true
    $difCol = @{}
    if ($srcFound -and $dstFound) {
        foreach ($colPName in $colPropertyNames) {
            $same = ($srcCol."$colPName") -eq ($dstCol."$colPName")
            $difCol[$colPName] = $same
            $dataSame = $dataSame -and $same
        }
    }


    $compareTableColumns[$k] = [PsCustomObject]@{
        SourceDatabase = $srcCol.TABLE_CATALOG
        DestinationDatabase = $dstCol.TABLE_CATALOG
        ColumnName          = $k;
        FoundInSource       = $srcFound;
        FoundInDestination  = $dstFound;
        Identical           = $srcFound -and $dstFound -and $dataSame;
        PropertyDifferences = [PsCustomObject]$difCol;
    }

    $cnt_cur += 1.0
    Remove-Variable 'srcCol'
    Remove-Variable 'dstCol'
    Remove-Variable 'srcFound'
    Remove-Variable 'dstFound'
    Remove-Variable 'dataSame'
}
Write-Progress -Id 1 -Activity "Comparing table definitions" -PercentComplete 100.0 -Completed

# ---- TODO Table Triggers ---

# --- Views ---
# Getting view data
$queryViews = "SELECT TABLE_CATALOG as VIEW_CATALOG, CONCAT('[',TABLE_SCHEMA,'].[',TABLE_NAME,']') as VIEW_NAME, (SELECT definition FROM sys.sql_modules WHERE object_id = OBJECT_ID(CONCAT('[',TABLE_SCHEMA,'].[',TABLE_NAME,']'))) as VIEW_DEFINITION FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = N'VIEW'"

$srcViews = @{}
Invoke-Sql -SqlQuery $queryViews -ConnectionString $srcConString `
| Select-Object -Property VIEW_CATALOG, VIEW_NAME, @{Name = 'VIEW_DEFINITION'; Expression = { Minimize-SQLString -SqlString $_.VIEW_DEFINITION } } `
| ForEach-Object { $srcViews[$_.VIEW_NAME] = $($_ | Select-Object -Property *) }

$dstViews = @{}
Invoke-Sql -SqlQuery $queryViews -ConnectionString $dstConString `
| Select-Object -Property VIEW_CATALOG, VIEW_NAME, @{Name = 'VIEW_DEFINITION'; Expression = { Minimize-SQLString -SqlString $_.VIEW_DEFINITION } } `
| ForEach-Object { $dstViews[$_.VIEW_NAME] = $($_ | Select-Object -Property *) }

# Comparing view data
$allViewNames = $(@($srcViews.Keys) + @($dstViews.Keys)) | Sort-Object -Unique
$compareViews = @{}

[double]$cnt_cur = 0.0
[double]$cnt_tot = $allViewNames.Count
foreach ($viewName in $allViewNames) {
    [double]$prog = $cnt_cur / $cnt_tot
    [string]$stat = "$($prog.ToString('p')) | $($cnt_cur.ToString('#,##0')) / $($cnt_tot.ToString('#,##0'))"
    Write-Progress -Id 2 -Activity "Comparing view definitions" -Status $stat -CurrentOperation $viewName -PercentComplete $($prog * 100.0)

    $srcFound = $null -ne $srcViews[$viewName]
    $dstFound = $null -ne $dstViews[$viewName]
    $srcDefinition = $null
    $dstDefinition = $null

    if ($srcFound) { $srcDefinition = $srcViews[$viewName].VIEW_DEFINITION }
    if ($dstFound) { $dstDefinition = $dstViews[$viewName].VIEW_DEFINITION }
    $compareViews[$viewName] = [PsCustomObject]@{
        ViewName              = $viewName;
        SourceDatabase = $srcViews[$viewName].VIEW_CATALOG;
        DestinationDatabase = $dstViews[$viewName].VIEW_CATALOG;
        FoundInSource         = $srcFound;
        FoundInDestination    = $dstFound;
        Identical             = $srcFound -and $dstFound -and ($srcDefinition -ieq $dstDefinition);
        SourceDefinition      = $srcDefinition;
        DestinationDefinition = $dstDefinition;
    }

    $cnt_cur += 1.0
    Remove-Variable 'srcFound'
    Remove-Variable 'dstFound'
    Remove-Variable 'srcDefinition'
    Remove-Variable 'dstDefinition'
}
Write-Progress -Id 2 -Activity "Comparing view definitions" -PercentComplete 100.0 -Completed


# --- Stored Procedures ---
# Getting stored procedure data
$queryProcedures = "SELECT ROUTINE_CATALOG as PROCEDURE_CATALOG, CONCAT('[',ROUTINE_SCHEMA,'].[',ROUTINE_NAME,']') as [PROCEDURE_NAME], ROUTINE_DEFINITION AS PROCEDURE_DEFINITION  FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE' AND ROUTINE_BODY = 'SQL'"

$srcProcedures = @{}
Invoke-Sql -SqlQuery $queryProcedures -ConnectionString $srcConString `
| Select-Object -Property PROCEDURE_CATALOG, @{Name = 'PROCEDURE_NAME'; Expression = { $_.PROCEDURE_NAME } }, @{Name = 'PROCEDURE_DEFINITION'; Expression = { Minimize-SQLString -SqlString $_.PROCEDURE_DEFINITION } } `
| ForEach-Object { $srcProcedures[$_.PROCEDURE_NAME] = $($_ | Select-Object -Property *) }

$dstProcedures = @{}
Invoke-Sql -SqlQuery $queryProcedures -ConnectionString $dstConString `
| Select-Object -Property PROCEDURE_CATALOG, @{Name = 'PROCEDURE_NAME'; Expression = { $_.PROCEDURE_NAME } }, @{Name = 'PROCEDURE_DEFINITION'; Expression = { Minimize-SQLString -SqlString $_.PROCEDURE_DEFINITION } } `
| ForEach-Object { $dstProcedures[$_.PROCEDURE_NAME] = $($_ | Select-Object -Property *) }

# comparing stored procedures
$allProcedureNames = $(@($srcProcedures.Keys) + @($dstProcedures.Keys)) | Sort-Object -Unique
$compareProcedures = @{}

[double]$cnt_cur = 0.0
[double]$cnt_tot = $allProcedureNames.Count
foreach ($procedureName in $allProcedureNames) {
    [double]$prog = $cnt_cur / $cnt_tot
    [string]$stat = "$($prog.ToString('p')) | $($cnt_cur.ToString('#,##0')) / $($cnt_tot.ToString('#,##0'))"
    Write-Progress -Id 3 -Activity "Comparing stored procedure definitions" -Status $stat -CurrentOperation $procedureName -PercentComplete $($prog * 100.0)

    $srcFound = $null -ne $srcProcedures[$procedureName]
    $dstFound = $null -ne $dstProcedures[$procedureName]
    $srcDefinition = $null
    $dstDefinition = $null

    if ($srcFound) { $srcDefinition = $srcProcedures[$procedureName].PROCEDURE_DEFINITION }
    if ($dstFound) { $dstDefinition = $dstProcedures[$procedureName].PROCEDURE_DEFINITION }
    $compareProcedures[$procedureName] = [PsCustomObject]@{
        ProcedureName         = $procedureName;
        SourceDatabase = $srcProcedures[$procedureName].PROCEDURE_CATALOG;
        DestinationDatabase = $dstProcedures[$procedureName].PROCEDURE_CATALOG;
        FoundInSource         = $srcFound;
        FoundInDestination    = $dstFound;
        Identical             = $srcFound -and $dstFound -and ($srcDefinition -ieq $dstDefinition);
        SourceDefinition      = $srcDefinition;
        DestinationDefinition = $dstDefinition;
    }

    $cnt_cur += 1.0
    Remove-Variable 'srcFound'
    Remove-Variable 'dstFound'
    Remove-Variable 'srcDefinition'
    Remove-Variable 'dstDefinition'
}
Write-Progress -Id 3 -Activity "Comparing stored procedure definitions" -PercentComplete 100.0 -Completed
# --- Functions ---
# Get functions data
$queryFunctions = "SELECT ROUTINE_CATALOG as FUNCTION_CATALOG, CONCAT('[',ROUTINE_SCHEMA,'].[',ROUTINE_NAME,']') as [FUNCTION_NAME], ROUTINE_DEFINITION AS FUNCTION_DEFINITION FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'FUNCTION' AND ROUTINE_BODY = 'SQL'"

$srcFunctions = @{}
Invoke-Sql -SqlQuery $queryFunctions -ConnectionString $srcConString `
| Select-Object -Property FUNCTION_CATALOG, @{Name = 'FUNCTION_NAME'; Expression = { $_.FUNCTION_NAME } }, @{Name = 'FUNCTION_DEFINITION'; Expression = { Minimize-SQLString -SqlString $_.FUNCTION_DEFINITION } } `
| ForEach-Object { $srcFunctions[$_.FUNCTION_NAME] = $($_ | Select-Object -Property *) }

$dstFunctions = @{}
Invoke-Sql -SqlQuery $queryFunctions -ConnectionString $dstConString `
| Select-Object -Property FUNCTION_CATALOG, @{Name = 'FUNCTION_NAME'; Expression = { $_.FUNCTION_NAME } }, @{Name = 'FUNCTION_DEFINITION'; Expression = { Minimize-SQLString -SqlString $_.FUNCTION_DEFINITION } } `
| ForEach-Object { $dstFunctions[$_.FUNCTION_NAME] = $($_ | Select-Object -Property *) }

# compare functions
$allFunctionNames = $(@($srcFunctions.Keys) + @($dstFunctions.Keys)) | Sort-Object -Unique
$compareFunctions = @{}

[double]$cnt_cur = 0.0
[double]$cnt_tot = $allFunctionNames.Count
foreach ($functionName in $allFunctionNames) {
    [double]$prog = $cnt_cur / $cnt_tot
    [string]$stat = "$($prog.ToString('p')) | $($cnt_cur.ToString('#,##0')) / $($cnt_tot.ToString('#,##0'))"
    Write-Progress -Id 4 -Activity "Comparing function definitions" -Status $stat -CurrentOperation $functionName -PercentComplete $($prog * 100.0)

    $srcFound = $null -ne $srcFunctions[$functionName]
    $dstFound = $null -ne $dstFunctions[$functionName]
    $dataSame = $false
    $srcDefinition = $null
    $dstDefinition = $null

    if ($srcFound) { $srcDefinition = $srcFunctions[$functionName].FUNCTION_DEFINITION }
    if ($dstFound) { $dstDefinition = $dstFunctions[$functionName].FUNCTION_DEFINITION }
    $compareFunctions[$functionName] = [PsCustomObject]@{
        FunctionName          = $functionName;
        SourceDatabase = $srcFunctions[$functionName].FUNCTION_CATALOG;
        DestinationDatabase = $dstFunctions[$functionName].FUNCTION_CATALOG;
        FoundInSource         = $srcFound;
        FoundInDestination    = $dstFound;
        Identical             = $srcFound -and $dstFound -and ($srcDefinition -ieq $dstDefinition);
        SourceDefinition      = $srcDefinition;
        DestinationDefinition = $dstDefinition;
    }

    $cnt_cur += 1.0
    Remove-Variable 'srcFound'
    Remove-Variable 'dstFound'
    Remove-Variable 'srcDefinition'
    Remove-Variable 'dstDefinition'
}
Write-Progress -Id 4 -Activity "Comparing function definitions" -PercentComplete 100.0 -Completed

# --- FINAL OUTPUT ---
$outputTable = @{}

# Table columns
$outputTable["TableColumns"] = @{}
$outputTable["TableColumns"]["Identical"] = $compareTableColumns.Values | Where-Object {$_.Identical} | Select-Object -Property ColumnName, SourceDatabase, DestinationDatabase
$outputTable["TableColumns"]["NotFoundInSource"] = $compareTableColumns.Values | Where-Object {-not $_.FoundInSource} | Select-Object -Property ColumnName, SourceDatabase, DestinationDatabase
$outputTable["TableColumns"]["NotFoundInDestination"] = $compareTableColumns.Values | Where-Object {-not $_.FoundInDestination} | Select-Object -Property ColumnName, SourceDatabase, DestinationDatabase
$outputTable["TableColumns"]["DataDiffers"] = $compareTableColumns.Values | Where-Object {(-not $_.Identical) -and ($_.FoundInSource) -and ($_.FoundInDestination)} | Select-Object -Property ColumnName, SourceDatabase, DestinationDatabase, PropertyDifferences
 
# views
$outputTable["VIEW"] = @{}
$outputTable["VIEW"]["Identical"] = $compareViews.Values | Where-Object {$_.Identical} | Select-Object -Property ViewName, SourceDatabase, DestinationDatabase
$outputTable["VIEW"]["NotFoundInSource"] = $compareViews.Values | Where-Object {-not $_.FoundInSource} | Select-Object -Property ViewName, SourceDatabase, DestinationDatabase
$outputTable["VIEW"]["NotFoundInDestination"] = $compareViews.Values | Where-Object {-not $_.FoundInDestination} | Select-Object -Property ViewName, SourceDatabase, DestinationDatabase
$outputTable["VIEW"]["DataDiffers"] = $compareViews.Values | Where-Object {(-not $_.Identical) -and ($_.FoundInSource) -and ($_.FoundInDestination)} | Select-Object -Property ViewName, SourceDatabase, DestinationDatabase, SourceDefinition, DestinationDefinition

# stored procedures
$outputTable["PROCEDURE"] = @{}
$outputTable["PROCEDURE"]["Identical"] = $compareProcedures.Values | Where-Object {$_.Identical} | Select-Object -Property ProcedureName, SourceDatabase, DestinationDatabase
$outputTable["PROCEDURE"]["NotFoundInSource"] = $compareProcedures.Values | Where-Object {-not $_.FoundInSource} | Select-Object -Property ProcedureName, SourceDatabase, DestinationDatabase
$outputTable["PROCEDURE"]["NotFoundInDestination"] = $compareProcedures.Values | Where-Object {-not $_.FoundInDestination} | Select-Object -Property ProcedureName, SourceDatabase, DestinationDatabase
$outputTable["PROCEDURE"]["DataDiffers"] = $compareProcedures.Values | Where-Object {(-not $_.Identical) -and ($_.FoundInSource) -and ($_.FoundInDestination)} | Select-Object -Property ProcedureName, SourceDatabase, DestinationDatabase, SourceDefinition, DestinationDefinition

# functions
$outputTable["FUNCTION"] = @{}
$outputTable["FUNCTION"]["Identical"] = $compareFunctions.Values | Where-Object {$_.Identical} | Select-Object -Property FunctionName, SourceDatabase, DestinationDatabase
$outputTable["FUNCTION"]["NotFoundInSource"] = $compareFunctions.Values | Where-Object {-not $_.FoundInSource} | Select-Object -Property FunctionName, SourceDatabase, DestinationDatabase
$outputTable["FUNCTION"]["NotFoundInDestination"] = $compareFunctions.Values | Where-Object {-not $_.FoundInDestination} | Select-Object -Property FunctionName, SourceDatabase, DestinationDatabase
$outputTable["FUNCTION"]["DataDiffers"] = $compareFunctions.Values | Where-Object {(-not $_.Identical) -and ($_.FoundInSource) -and ($_.FoundInDestination)} | Select-Object -Property FunctionName, SourceDatabase, DestinationDatabase, SourceDefinition, DestinationDefinition



$outputJsonPath = Join-Path -Path $PSScriptRoot -ChildPath "Compare-DBStructure.json"
$outputJsonString = $outputTable | ConvertTo-Json -Depth 10 -Compress
Set-Content -Value $outputJsonString -Path $outputJsonPath -Force -Encoding UTF8
Write-Host "Saved output as json to: $outputJsonPath"