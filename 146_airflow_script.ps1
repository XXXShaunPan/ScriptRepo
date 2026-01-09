$session = New-Object Microsoft.PowerShell.Commands.WebRequestSession

function Login_Airflow {
    # $existingCookie = ($session.Cookies.GetCookies("http://34.142.225.103:8080") | 
    #               Where-Object Name -eq "session")
    
    # if ($existingCookie) {
    #     $temp_cookie = $existingCookie[0].Value

    #     $session.Cookies = New-Object System.Net.CookieContainer

    #     $newCookie = New-Object System.Net.Cookie("session", $temp_cookie, "/", "34.142.225.103")
    #     $newCookie.HttpOnly = $true
    #     $session.Cookies.Add($newCookie)
    #     return
    # }
    $session.Cookies = New-Object System.Net.CookieContainer
    $loginUrl = "http://34.142.225.103:8080/login/?next=http://34.142.225.103:8080/home"
    $response = Invoke-WebRequest -Uri $loginUrl -WebSession $session -UseBasicParsing
    $htmlContent = $response.Content
    $pattern = 'name="csrf_token" type="hidden" value="([^"]+)"'
    $match = $htmlContent | Select-String -Pattern $pattern
    $token = $match.Matches.Groups[1].Value

    $headers = @{
        "Accept" = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
        "Accept-Language" = "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6"
        "Content-Type" = "application/x-www-form-urlencoded"
        "Referer" = "http://34.142.225.103:8080/login/?next=http%3A%2F%2F34.142.225.103%3A8080%2Fhome"
        "User-Agent" = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    }

    $body = @{
        "csrf_token" = $token
        "username" = "shaun"
        "password" = "shaun2024"
        "next" = "http://34.142.225.103:8080/home"
    }

    $loginPostUrl = "http://34.142.225.103:8080/login/"
    $response = Invoke-WebRequest -Uri $loginPostUrl -WebSession $session -Method POST -Headers $headers -Body $body -UseBasicParsing -ContentType "application/x-www-form-urlencoded"
}


function run_a_dag {
    param (
        [string]$dag_id = 'paid_ads_automation',
        [string]$task_id,
        [string]$conf_dict
    )

    $body = @{
        "conf" = $conf_dict | ConvertFrom-Json  
        "dag_run_id" = $task_id
    } | ConvertTo-Json -Depth 10
    $bodyBytes = [System.Text.Encoding]::UTF8.GetBytes($body)
    $headers = @{
        "Accept" = "application/json"
        "Content-Type" = "application/json; charset=utf-8"
    }

    $url = "http://34.142.225.103:8080/api/v1/dags/$dag_id/dagRuns"
    try {
        $response = Invoke-RestMethod -Uri $url -Method Post -Headers $headers -Body $bodyBytes -WebSession $session -UseBasicParsing -ContentType "application/json"
        # Write-Host "请求成功: $response"
        return $response
    }
    catch {
        Write-Error "请求失败: $_.Exception.Response"
        throw $_
    }
}

function get_all_dags {

    $url = "http://34.142.225.103:8080/api/v1/dags?limit=1000&only_active=true&paused=false"
    
    $headers = @{
        "Accept" = "application/json"
    }
    try {
        $response = Invoke-RestMethod -Uri $url -Method Get -Headers $headers -WebSession $session -UseBasicParsing -ContentType "application/json"
        return $response.dags | Where-Object { $_.owners -contains 'Shaun' }
    }
    catch {
        Write-Error "请求失败: $_.Exception.Message"
        throw $_
    }
}


function wait_for_running {
    param (
        [string]$dag_id = 'paid_ads_automation',
        [string]$dag_run_id
    )

    $url = "http://34.142.225.103:8080/api/v1/dags/$dag_id/dagRuns/$dag_run_id"
    
    $headers = @{
        "Accept" = "application/json"
    }
    $res = 'queued'
    while ($res -eq 'queued') {
        try {
            $response = Invoke-RestMethod -Uri $url -Method Get -Headers $headers -WebSession $session -UseBasicParsing -ContentType "application/json"
            $res = $response.state
            write-host "已存放队列，等待执行中... $res" -ForegroundColor yellow -BackgroundColor black
            Start-Sleep -Seconds 1
        }
    catch {
            Write-Error "请求失败: $_.Exception.Message"
            throw $_
        }
    }
}

function get_dag_runs_status {

    param (
        [string]$dag_id = 'paid_ads_automation',
        [string]$offset = 0,
        [string]$execution_date = '2025-05-26T03:00:06.796574+00:00'
    )
    $uri = "http://34.142.225.103:8080/get_logs_with_metadata"
    $params =@{
        'dag_id' = $dag_id
        'task_id' = $dag_id
        'map_index' = '-1'
        'execution_date' = $execution_date
        'try_number' = '1'
        'metadata' = "{`"end_of_log`":false,`"log_pos`":$offset}"  # 双引号 + 转义内部双引号
    }
    
    $headers = @{
        'Accept' = '*/*'
        'Accept-Language' = 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6'
        'User-Agent' = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0'
        'X-Requested-With' = 'XMLHttpRequest'
    }
    
    try {
        $response = Invoke-RestMethod -Uri $uri -Method Get -Headers $headers -WebSession $session -Body $params -UseBasicParsing -ContentType "application/json"
        return $response
    } catch {
        Write-Host "An error occurred:"
        Write-Host $_.Exception.Message
    }
}

function main_for_all_dags {
    Login_Airflow
    $dags = get_all_dags
    $dags | ForEach-Object -Begin { $i = 0 } -Process {
        $_ | Select-Object @{Name='序号'; Expression={ $i }}, 
                           dag_id, 
                           description
        $i++
    } | Format-Table -AutoSize
    # 手动键入参数
    $dag_index = Read-Host "请输入要执行的脚本序号"
    $dag_id = $dags[$dag_index].dag_id
    $conf_dict = "{}"
    if ($dag_id -eq 'lvt_testing_create_ads') {
        @{
            'VN' = 1
            'MY' = 2
            'TH' = 3
            'BR' = 4
            'PH' = 5
        }.GetEnumerator() | 
        Select-Object @{Name='Site'; Expression={$_.Key}}, 
                    @{Name='序号'; Expression={$_.Value}} |
        Format-Table -AutoSize
        $sites = Read-Host "请输入要跑的站点序号（如 123）"
        $conf_dict = "{'sites':'$sites'}"
    }
    $task_id = "$env:USERNAME$(Get-Date -Format 'yyyy-MM-dd-HH-mm-ss')"
    write-host "task_id: $task_id"
    $dag_runs = run_a_dag -dag_id $dag_id -task_id $task_id -conf_dict $conf_dict
    wait_for_running -dag_id $dag_id -dag_run_id $task_id
    $is_continue = $true
    $offset = 0
    # 登录airflow
    Login_Airflow
    while ($is_continue) {
        $dag_result = get_dag_runs_status -dag_id $dag_id -offset $offset -execution_date $dag_runs.execution_date
        $offset = $dag_result.metadata.log_pos
        if ($offset -gt 0) {
            $is_continue = -not $dag_result.metadata.end_of_log
            # 输出log
            $result = $dag_result.message[0][1] -split "`n"
            $result | Select-String -Pattern "ads_|logging_mixin" | ForEach-Object {
                $line = $_.Line.Trim()
                
                switch -Regex ($line.ToLower()) {
                    'ERROR' { 
                        Write-Host $line -ForegroundColor Red -BackgroundColor Black
                    }
                    'WARNING' { 
                        Write-Host $line -ForegroundColor Yellow -BackgroundColor Black
                    }
                    'INFO' { 
                        Write-Host $line -ForegroundColor Green -BackgroundColor Black
                    }
                    default {
                        Write-Host $line -ForegroundColor Cyan -BackgroundColor Black
                    }
                }
            }
        }
            Start-Sleep -Seconds 2
    }
}
main_for_all_dags
write-host "done" -ForegroundColor green -BackgroundColor black
[System.Media.SystemSounds]::Hand.Play()
pause
