# 本地密钥与 Git 提交

Java Workflow 和 Console Hub 的真实 API 密钥保存在根目录的 `.local/secrets.properties`，此目录不参与 Git 提交。源码配置使用环境变量占位符；运行环境中的同名环境变量可以覆盖本地配置。

新机器可将 `secrets.properties.example` 复制到 `.local/secrets.properties` 后填写密钥。Spring Boot 从项目根目录或各 Java 模块目录启动时，会自动加载该文件。容器部署请设置对应环境变量，或通过 Spring 的 `SPRING_CONFIG_IMPORT` 指定私有配置文件。

Python 服务的 `config.env` 保留在本地，不受 Git 跟踪；仓库仅保存 `config.env.example`。Python 基准代码按项目约定保留不改。`core/workflow/service/app_service.py` 的本地基准含有旧硬编码密钥，因此当前工作区对该文件设置了 `skip-worktree`，Git 索引和 GitHub 保留脱敏版本。不要取消此标记后直接添加该文件；提交检查也会拦截已知原密钥。其他克隆不会继承这个本地标记，取得的是脱敏文件；部署前仍需配置相应应用认证信息。

旧 Git 历史已备份到 `.local/security-backup/before-secret-protection.bundle`，仅供本机恢复。该备份包含原密钥，禁止上传或分享。工作区的 `master` 已衔接 GitHub 的脱敏初始提交，不应从备份恢复旧分支并推送。

在新克隆中启用检查：

```powershell
./scripts/install-secret-hooks.ps1
```

其他平台可运行 `git config core.hooksPath .githooks`。检查需要可用的 `python` 命令。

提交检查读取 Git 暂存区，推送检查读取待推送分支的全部历史，阻止已知原密钥、常见提供商令牌、疑似硬编码凭据以及私有配置文件。错误信息只显示文件位置，不输出密钥。可以手动运行：

```powershell
python scripts/check-secrets.py --staged
python scripts/check-secrets.py --history HEAD
```

GitHub Actions 也会扫描提交历史，但它在上传后执行，不能替代本地检查。Git hooks 不是不可绕过的安全边界，`--no-verify`、禁用 hooks 或未安装 hooks 都可能跳过检查；扫描规则也无法识别所有未知格式的秘密。
