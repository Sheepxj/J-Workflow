# Java Mock 工作流 JMeter 压测

## 测试目标

- flowId：`826202600000000001`
- 接口：`POST /api/workflow/chat`
- 工作流：`START -> 10 x Mock(100ms) -> END`
- 默认 Ramp-up：30 秒
- 默认持续时间：300 秒
- 完成条件：HTTP 200 且 SSE 响应包含 `"progress":1.0`

## 单档运行

```powershell
C:\tools\apache-jmeter-5.6.3\bin\jmeter.bat -n `
  -t "E:\Sheepxj\Code\PaiFlow-main\core-workflow-java\load-test\mock-workflow-10-threads.jmx" `
  -Jhost=localhost `
  -Jport=7880 `
  -Jduration=300 `
  -JrampUp=30 `
  -l "E:\temp\mock-workflow-10-threads.jtl" `
  -e `
  -o "E:\temp\mock-workflow-report-10"
```

## 五档批量运行

```powershell
.\run-mock-load-tests.ps1 `
  -JMeterHome "C:\tools\apache-jmeter-5.6.3" `
  -TargetHost "localhost" `
  -TargetPort 7880 `
  -DurationSeconds 300 `
  -RampUpSeconds 30
```

脚本按照 10、20、30、40、50 并发依次执行，每档生成独立 JTL 和 HTML 报告。
