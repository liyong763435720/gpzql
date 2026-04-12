; ============================================================
;  涌金阁 - 多市场量化分析平台  Inno Setup 安装脚本
;  构建前请先运行 build_installer.bat 准备好 python-embed 目录
; ============================================================

#define AppName      "涌金阁"
#define AppVersion   "1.0.0"
#define AppPublisher "涌金阁"
#define AppURL       "http://localhost:8588"
#define AppExeName   "涌金阁.exe"
#define ServiceName  "YongJinGe"

[Setup]
AppId={{A3F2B8C1-9D4E-4F7A-B2C3-D5E6F7A8B9C0}
AppName={#AppName}
AppVersion={#AppVersion}
AppPublisher={#AppPublisher}
AppPublisherURL={#AppURL}
AppSupportURL={#AppURL}
DefaultDirName={autopf}\{#AppName}
DefaultGroupName={#AppName}
AllowNoIcons=yes
OutputDir=dist
OutputBaseFilename=涌金阁_Setup_v{#AppVersion}
SetupIconFile=assets\icon.ico
Compression=lzma2/ultra64
SolidCompression=yes
WizardStyle=modern
PrivilegesRequired=admin
; 中文界面
ShowLanguageDialog=no
LanguageDetectionMethod=none

[Languages]
Name: "chinesesimplified"; MessagesFile: "ChineseSimplified.isl"

[Tasks]
Name: "desktopicon"; Description: "创建桌面快捷方式"; GroupDescription: "附加任务:"; Flags: unchecked

[Files]
; Python 嵌入式环境
Source: "python-embed\*"; DestDir: "{app}\python"; Flags: ignoreversion recursesubdirs createallsubdirs

; 主应用代码
Source: "..\app\*";       DestDir: "{app}\app";       Flags: ignoreversion recursesubdirs createallsubdirs; Excludes: "__pycache__,*.pyc"
Source: "..\static\*";    DestDir: "{app}\static";    Flags: ignoreversion recursesubdirs createallsubdirs
Source: "..\templates\index.html";         DestDir: "{app}\templates"; Flags: ignoreversion
Source: "..\templates\index_desktop.html"; DestDir: "{app}\templates"; Flags: ignoreversion
Source: "..\lof1\*";      DestDir: "{app}\lof1";      Flags: ignoreversion recursesubdirs createallsubdirs; Excludes: "*.db,*.db-journal,__pycache__"
Source: "..\main.py";     DestDir: "{app}";            Flags: ignoreversion
Source: "..\start_prod.py"; DestDir: "{app}";          Flags: ignoreversion
Source: "..\lof1_wrapper.py"; DestDir: "{app}";        Flags: ignoreversion
Source: "..\requirements.txt"; DestDir: "{app}";       Flags: ignoreversion

; 文档
Source: "..\USAGE.md"; DestDir: "{app}"; Flags: ignoreversion

; 配置示例（不覆盖已有配置）
Source: "..\config.json.example"; DestDir: "{app}"; DestName: "config.json.example"; Flags: ignoreversion
Source: "..\config.json.example"; DestDir: "{app}"; DestName: "config.json"; Flags: onlyifdoesntexist

; NSSM 服务管理工具
Source: "assets\nssm.exe"; DestDir: "{app}"; Flags: ignoreversion

; 桌面启动器（原生窗口，替代浏览器）
Source: "assets\涌金阁.exe"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
Name: "{group}\{#AppName}";           Filename: "{app}\涌金阁.exe"; WorkingDir: "{app}"; IconFilename: "{app}\涌金阁.exe"
Name: "{group}\卸载 {#AppName}";      Filename: "{uninstallexe}"
Name: "{commondesktop}\{#AppName}";  Filename: "{app}\涌金阁.exe"; WorkingDir: "{app}"; IconFilename: "{app}\涌金阁.exe"; Tasks: desktopicon

[Run]
; 安装 Python 依赖（日志写到 install.log，方便排查失败原因）
Filename: "{app}\python\python.exe"; \
    Parameters: "-m pip install --no-index --find-links ""{app}\python\packages"" -r ""{app}\requirements.txt"" --log ""{app}\install.log"""; \
    StatusMsg: "安装依赖包（首次较慢，请耐心等待）..."; Flags: runhidden waituntilterminated

; 注册 Windows 服务（必选，start_prod.py 路径加引号避免路径含空格报错）
Filename: "{app}\nssm.exe"; Parameters: "install {#ServiceName} ""{app}\python\python.exe"""; \
    StatusMsg: "注册系统服务..."; Flags: runhidden waituntilterminated
Filename: "{app}\nssm.exe"; Parameters: "set {#ServiceName} AppParameters """"""{app}\start_prod.py"""""""; \
    Flags: runhidden waituntilterminated
Filename: "{app}\nssm.exe"; Parameters: "set {#ServiceName} AppDirectory ""{app}"""; \
    Flags: runhidden waituntilterminated
Filename: "{app}\nssm.exe"; Parameters: "set {#ServiceName} DisplayName ""涌金阁 - 多市场量化分析平台"""; \
    Flags: runhidden waituntilterminated
Filename: "{app}\nssm.exe"; Parameters: "set {#ServiceName} AppEnvironmentExtra YONGJINGE_DESKTOP=1"; \
    Flags: runhidden waituntilterminated
Filename: "{app}\nssm.exe"; Parameters: "set {#ServiceName} Start SERVICE_AUTO_START"; \
    Flags: runhidden waituntilterminated
Filename: "{app}\nssm.exe"; Parameters: "start {#ServiceName}"; \
    Flags: runhidden waituntilterminated

; 安装完成后打开桌面窗口
Filename: "{app}\涌金阁.exe"; Description: "立即打开涌金阁"; Flags: postinstall nowait skipifsilent

[UninstallRun]
; 卸载时停止并删除服务
Filename: "{app}\nssm.exe"; Parameters: "stop {#ServiceName}";    Flags: runhidden waituntilterminated
Filename: "{app}\nssm.exe"; Parameters: "remove {#ServiceName} confirm"; Flags: runhidden waituntilterminated

[UninstallDelete]
; 卸载时清理运行时生成的文件
Type: files; Name: "{app}\install.dat"
Type: files; Name: "{app}\update_progress.json"
Type: files; Name: "{app}\update_checkpoint.json"
Type: files; Name: "{app}\install.log"
Type: files; Name: "{app}\service_stdout.log"
Type: files; Name: "{app}\service_stderr.log"

[Code]
var
  DeleteAllData: Boolean;

// 安装前检查：是否已有旧版本服务在运行
procedure StopExistingService();
var
  ResultCode: Integer;
begin
  Exec(ExpandConstant('{app}\nssm.exe'),
       'stop {#ServiceName}',
       '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
end;

procedure CurStepChanged(CurStep: TSetupStep);
begin
  if CurStep = ssInstall then
    StopExistingService();
end;

// 卸载前询问是否删除所有数据
procedure InitializeUninstallProgressForm();
begin
  // 此处留空，询问在 InitializeUninstall 中完成
end;

function InitializeUninstall(): Boolean;
begin
  Result := True;
  DeleteAllData := False;
  if MsgBox(
    '是否同时删除所有用户数据？' + #13#10 + #13#10 +
    '包括：数据库、配置文件、授权文件、日志等。' + #13#10 +
    '此操作不可撤销。' + #13#10 + #13#10 +
    '点击【是】：完全清除所有数据' + #13#10 +
    '点击【否】：仅卸载程序，保留数据',
    mbConfirmation, MB_YESNO) = IDYES then
  begin
    DeleteAllData := True;
  end;
end;

procedure CurUninstallStepChanged(CurUninstallStep: TUninstallStep);
var
  AppDir: String;
begin
  if (CurUninstallStep = usPostUninstall) and DeleteAllData then
  begin
    AppDir := ExpandConstant('{app}');
    // 删除整个安装目录（含数据库、配置、日志等所有内容）
    DelTree(AppDir, True, True, True);
  end;
end;
