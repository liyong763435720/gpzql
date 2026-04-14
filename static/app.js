// 全局变量
let progressEventSource = null;
let currentUser = null;
let _licenseValid = false;

// 预取缓存：登录后后台悄悄拉取，用户点 tab 时直接命中
const _prefetchCache = {};

function _prefetchAll() {
    // 预取点数流水
    fetch('/api/credits/transactions?page=1', { credentials: 'include' })
        .then(r => r.json())
        .then(d => { if (d.success) _prefetchCache['credits_tx'] = d.data; })
        .catch(() => {});
    // 预取我的订单
    fetch('/api/my/orders', { credentials: 'include' })
        .then(r => r.json())
        .then(d => { if (d.success) _prefetchCache['my_orders'] = d.data; })
        .catch(() => {});
}

// 单股分析：单月/按月表单切换（全局函数，HTML onclick 直接调用，不依赖初始化时机）
function stockAnalysisShowSingle() {
    const s = document.getElementById('stock-single-month-form');
    const m = document.getElementById('stock-multi-month-form');
    if (s) s.style.display = 'block';
    if (m) m.style.display = 'none';
}
function stockAnalysisShowMulti() {
    const s = document.getElementById('stock-single-month-form');
    const m = document.getElementById('stock-multi-month-form');
    if (s) s.style.display = 'none';
    if (m) m.style.display = 'block';
}

// 页面加载时初始化
document.addEventListener('DOMContentLoaded', function() {
    // 支付成功跳转过来时显示提示
    if (new URLSearchParams(window.location.search).get('payment') === 'success') {
        const tip = document.getElementById('payment-success-tip');
        if (tip) tip.style.display = '';
    }
    checkLicenseStatus();   // 先检查授权，再检查登录
    initDefaultYears();
    initStockAutocomplete(); // 桌面模式 main-content 直接可见，需提前绑定，不能等登录流程
});

// ══════════════════════════════════════════════════
//  授权管理
// ══════════════════════════════════════════════════

async function checkLicenseStatus() {
    try {
        const res = await fetch('/api/license/status');
        const result = await res.json();
        const d = result.data || {};
        _licenseValid = d.valid === true;

        if (d.status === 'trial') {
            // 试用期内：显示顶部提醒条，正常进入
            const bar = document.getElementById('trial-topbar');
            const daysEl = document.getElementById('trial-topbar-days');
            if (bar && daysEl) {
                daysEl.textContent = d.trial_remaining;
                bar.style.display = '';
                // 顶部内容区下移，避免被遮挡
                const main = document.getElementById('main-content') || document.querySelector('.main-content');
                if (main) main.style.paddingTop = (parseInt(getComputedStyle(main).paddingTop) + 38) + 'px';
            }
            checkLoginStatus();
        } else if (!d.valid) {
            // 未激活或已过期：弹出激活遮罩
            _showLicenseOverlayWithStatus(d);
        } else {
            // 已激活
            checkLoginStatus();
        }
    } catch (e) {
        console.error('License check failed', e);
        checkLoginStatus();
    }
}

function _showLicenseOverlayWithStatus(d) {
    const overlay = document.getElementById('license-overlay');
    if (!overlay) return;

    // 机器码
    const mid = document.getElementById('lic-machine-id');
    if (mid) mid.value = d.machine_id || '';

    // 状态提示
    const trialBanner = document.getElementById('lic-trial-banner');
    const errBanner   = document.getElementById('lic-error-banner');
    const errText     = document.getElementById('lic-error-text');
    const trialBtn    = document.getElementById('lic-trial-btn');
    const trialDays   = document.getElementById('lic-trial-days');
    const trialText   = document.getElementById('lic-trial-text');

    if (d.status === 'trial_expired') {
        if (errBanner) { errBanner.style.display=''; errText.textContent = '试用期已结束，请激活后继续使用。'; }
    } else if (d.status === 'expired') {
        if (errBanner) { errBanner.style.display=''; errText.textContent = '授权已过期，请联系开发者续期。'; }
    } else if (d.status === 'machine_mismatch') {
        if (errBanner) { errBanner.style.display=''; errText.textContent = '激活码与本机不匹配，请重新获取。'; }
    }

    overlay.style.display = 'flex';
}

function showLicenseOverlay() {
    fetch('/api/license/status').then(r=>r.json()).then(result => {
        _showLicenseOverlayWithStatus(result.data || {});
    });
}

function closeLicenseOverlay() {
    const overlay = document.getElementById('license-overlay');
    if (overlay) overlay.style.display = 'none';
    checkLoginStatus();
}

function copyMachineId() {
    const val = document.getElementById('lic-machine-id')?.value;
    if (!val) return;
    navigator.clipboard.writeText(val).then(() => {
        alert('机器码已复制：' + val);
    }).catch(() => {
        prompt('请手动复制机器码：', val);
    });
}

async function submitLicense() {
    const licenseText = (document.getElementById('lic-input')?.value || '').trim();
    if (!licenseText) { alert('请先粘贴激活码'); return; }

    const btn = document.getElementById('lic-submit-btn');
    if (btn) { btn.disabled = true; btn.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>验证中...'; }

    const errBanner = document.getElementById('lic-error-banner');
    const errText   = document.getElementById('lic-error-text');
    const okBanner  = document.getElementById('lic-success-banner');
    const okText    = document.getElementById('lic-success-text');
    if (errBanner) errBanner.style.display = 'none';
    if (okBanner)  okBanner.style.display  = 'none';

    try {
        const res = await fetch('/api/license/activate', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ license_text: licenseText })
        });
        const result = await res.json();
        if (result.success) {
            if (okBanner) { okBanner.style.display=''; okText.textContent = result.message; }
            // 隐藏试用条
            const bar = document.getElementById('trial-topbar');
            if (bar) bar.style.display = 'none';
            setTimeout(() => {
                document.getElementById('license-overlay').style.display = 'none';
                checkLoginStatus();
            }, 1500);
        } else {
            if (errBanner) { errBanner.style.display=''; errText.textContent = result.message; }
        }
    } catch (e) {
        if (errBanner) { errBanner.style.display=''; errText.textContent = '网络错误，请重试'; }
    } finally {
        if (btn) { btn.disabled = false; btn.innerHTML = '<i class="bi bi-unlock me-2"></i>立即激活'; }
    }
}

// 将所有结束年份输入框默认值设为当前年
function initDefaultYears() {
    const currentYear = new Date().getFullYear();
    ['stock-end-year', 'stock-multi-end-year', 'filter-end-year', 'industry-end-year', 'enhanced-end-year', 'menhanced-end-year'].forEach(id => {
        const el = document.getElementById(id);
        if (el && !el.value) el.value = currentYear;
    });
}

// 校验起始年份不大于结束年份
function validateYearRange(startId, endId) {
    const start = parseInt(document.getElementById(startId)?.value);
    const end = parseInt(document.getElementById(endId)?.value);
    if (isNaN(start) || isNaN(end)) { alert('请输入有效的年份'); return false; }
    if (start > end) { alert('起始年份不能大于结束年份'); return false; }
    return true;
}

// 页面标题映射
const TAB_TITLES = {
    'dashboard':          '数据概览',
    'stock-analysis':     '单股分析',
    'month-filter':       '月榜单',
    'industry-analysis':  '行业分析',
    'industry-enhanced':  '行业增强分析',
    'month-enhanced':     '月榜单增强',
    'source-compare':     '数据校对',
    'config':             '系统配置 · 数据源 & 更新',
    'config-lof-db':      '系统配置 · 数据管理',
    'user-management':    '系统配置 · 用户管理',
    'plan-management':    '系统配置 · 套餐管理',
    'payment-config':     '系统配置 · 支付配置',
    'lof-arbitrage':      'LOF基金套利',
    'help':               '使用指南',
    'my-subscription':    '我的订阅',
    'order-management':   '订单管理',
    'revenue-stats':      '收入统计',
};

// 各 tab 所需最低权限（未列出 = 免费可访问）
const TAB_PERM_REQUIRED = {
    'month-filter':      { perm: 'month_filter',        plan: '基础版' },
    'month-enhanced':    { perm: 'month_enhanced',      plan: '专业版' },
    'industry-analysis': { perm: 'industry_statistics', plan: '基础版' },
    'industry-enhanced': { perm: 'industry_enhanced',   plan: '专业版' },
    'source-compare':    { perm: 'source_compare',      plan: '专业版' },
    'lof-arbitrage':     { perm: 'lof_arbitrage',       plan: '专业版' },
};

// 显示标签页
function showTab(tabName, eventElement) {
    // 权限拦截：无权限时弹解锁/升级提示
    if (currentUser && currentUser.role !== 'admin') {
        const req = TAB_PERM_REQUIRED[tabName];
        if (req && !(currentUser.permissions || []).includes(req.perm)) {
            showUpgradeModal(req.plan, req.perm, tabName, eventElement);
            return;
        }
    }
    // 隐藏所有标签页
    document.querySelectorAll('.tab-content').forEach(tab => {
        tab.style.display = 'none';
    });
    // 显示选中的标签页
    document.getElementById(tabName).style.display = 'block';

    // 更新顶栏标题
    const titleEl = document.getElementById('topbar-title');
    if (titleEl) titleEl.textContent = TAB_TITLES[tabName] || '';

    // 更新导航栏活动状态
    document.querySelectorAll('.nav-link').forEach(link => {
        link.classList.remove('active');
    });
    // 激活当前项，并自动展开所在父菜单
    if (eventElement) {
        eventElement.classList.add('active');
        const submenu = eventElement.closest('.nav-submenu');
        if (submenu) {
            const parent = submenu.closest('.nav-parent');
            if (parent && !parent.classList.contains('open')) {
                parent.classList.add('open');
            }
        }
    }

    // 移动端：切换后关闭侧边栏（已禁用，保持侧边栏常驻）
    // if (window.closeMobileSidebar) closeMobileSidebar();

    // lof-arbitrage：首次切换时才加载 iframe，避免页面打开就发起请求
    if (tabName === 'lof-arbitrage') {
        const iframe = document.getElementById('lof-iframe');
        if (iframe && iframe.src === 'about:blank') {
            iframe.src = '/lof1/';
        }
    }

    // 切换到数据概览时自动刷新最新数据
    if (tabName === 'dashboard') {
        loadDataStatus();
        loadDashboardIndustryStats();
    }

    // 切换到用户管理/系统配置时重新加载最新配置（确保SMTP等字段不丢失）
    if ((tabName === 'config' || tabName === 'user-management') && currentUser && currentUser.role === 'admin') {
        loadSystemConfig();
    }

    // 切换到套餐管理时初始化 tab
    if (tabName === 'plan-management') {
        _planTabLoaded = { price: false, promo: false, trial: false, records: false };
        switchPlanTab('price', document.querySelector('#planMgmtTabs .nav-link'));
    }

    // 切换到公告管理时加载数据
    if (tabName === 'announcement-mgmt') loadAnnouncementList();

    // 切换到宕机补偿时加载数据
    if (tabName === 'outage-mgmt') loadOutageList();

    // 切换到数据管理时初始化 tab
    if (tabName === 'data-mgmt') {
        _dataTabLoaded = { backup: false, kline: false, lof: false };
        switchDataTab('backup', document.querySelector('#dataMgmtTabs .nav-link'));
    }

    // 切换到支付配置时加载配置
    if (tabName === 'payment-config') loadPaymentConfig();

    // 切换到我的订阅时加载数据
    if (tabName === 'my-subscription') loadMySubscription();
    if (tabName === 'my-credits') loadMyCredits();
    if (tabName === 'my-tickets') loadMyTickets();
    if (tabName === 'ticket-mgmt') loadAdminTickets('');

    // 切换到订单管理时加载数据
    if (tabName === 'order-management') loadAdminOrders(1);

    // 切换到收入统计时加载数据
    if (tabName === 'revenue-stats') loadRevenueStats();
}

// 切换侧边栏子菜单
function toggleNavSubmenu(linkEl) {
    const parent = linkEl.closest('.nav-parent');
    if (parent) parent.classList.toggle('open');
}

// ===== Dashboard 工具函数 =====
function _fmtDate(d) {
    if (!d || d.length !== 8) return '—';
    return `${d.slice(0,4)}-${d.slice(4,6)}-${d.slice(6,8)}`;
}
function _freshnessInfo(d) {
    if (!d || d.length !== 8) return { cls: 'text-muted', icon: '⬜', days: Infinity };
    const days = (Date.now() - new Date(`${d.slice(0,4)}-${d.slice(4,6)}-${d.slice(6,8)}`)) / 86400000;
    if (days <= 45)  return { cls: 'text-success fw-semibold', icon: '🟢', days };
    if (days <= 180) return { cls: 'text-warning fw-semibold', icon: '🟡', days };
    return { cls: 'text-danger fw-semibold', icon: '🔴', days };
}

// 从数据源同步参考股票总数
async function syncReferenceCountsFromDataSource() {
    const btn = document.getElementById('dash-sync-btn');
    if (btn) {
        btn.disabled = true;
        btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>同步中…';
    }
    try {
        const resp = await fetch('/api/data/sync-reference-counts', {
            method: 'POST',
            credentials: 'include'
        });
        const result = await resp.json();
        if (result.success) {
            const parts = Object.entries(result.data || {}).map(([k, v]) => `${k}: ${v.toLocaleString()}`).join('，');
            const syncAt = result.synced_at || '';
            alert(`参考总数已同步（${syncAt}）：${parts}`);
            loadDataStatus();
        } else {
            const errs = Object.entries(result.errors || {}).map(([k, v]) => `${k}: ${v}`).join('；');
            alert('同步失败' + (errs ? `：${errs}` : ''));
        }
    } catch (e) {
        alert('同步请求失败：' + e.message);
    } finally {
        if (btn) {
            btn.disabled = false;
            btn.innerHTML = '<i class="bi bi-arrow-repeat me-1"></i>从数据源同步参考总数';
        }
    }
}

// 刷新整个 Dashboard
function refreshDashboard() {
    loadDataStatus();
    loadDashboardIndustryStats();
    if (document.getElementById('lof-dashboard-section').style.display !== 'none') {
        loadLofStats();
        loadLofOpportunities();
    }
}

// 加载数据状态（重写为新布局）
async function loadDataStatus() {
    try {
        const response = await fetch('/api/data/status', { credentials: 'include' });
        const result = await response.json();
        if (!result.success) return;
        const data = result.data;
        const totalStocks = data.total_stocks || 0;
        const latestDate  = data.latest_date || null;
        const sources     = data.data_sources || [];
        const markets     = data.market_stats || [];
        const fi          = _freshnessInfo(latestDate);

        // ── KPI 卡片 ──
        const totalKlines = sources.reduce((s, x) => s + (x.data_count || 0), 0);
        const srcCount    = sources.filter(s => s.data_count > 0).length;
        document.getElementById('dash-kpi').innerHTML = `
            <div class="col-6 col-md-3">
                <div class="card text-center h-100 border-0 shadow-sm">
                    <div class="card-body py-3">
                        <div style="font-size:1.6rem;font-weight:700;color:#2563eb">${totalStocks.toLocaleString()}</div>
                        <div class="text-muted small mt-1"><i class="bi bi-collection me-1"></i>股票总数</div>
                    </div>
                </div>
            </div>
            <div class="col-6 col-md-3">
                <div class="card text-center h-100 border-0 shadow-sm">
                    <div class="card-body py-3">
                        <div style="font-size:1.6rem;font-weight:700;color:#2563eb">${totalKlines.toLocaleString()}</div>
                        <div class="text-muted small mt-1"><i class="bi bi-bar-chart me-1"></i>K线总条数</div>
                    </div>
                </div>
            </div>
            <div class="col-6 col-md-3">
                <div class="card text-center h-100 border-0 shadow-sm">
                    <div class="card-body py-3">
                        <div class="${fi.cls}" style="font-size:1.1rem;font-weight:700">${fi.icon} ${_fmtDate(latestDate)}</div>
                        <div class="text-muted small mt-1"><i class="bi bi-calendar-check me-1"></i>最新数据日期</div>
                    </div>
                </div>
            </div>
            <div class="col-6 col-md-3">
                <div class="card text-center h-100 border-0 shadow-sm">
                    <div class="card-body py-3">
                        <div style="font-size:1.6rem;font-weight:700;color:#2563eb">${srcCount}</div>
                        <div class="text-muted small mt-1"><i class="bi bi-hdd-stack me-1"></i>活跃数据源</div>
                    </div>
                </div>
            </div>`;

        // ── 数据完整度 ──
        // ── 数据完整度 ──
        const completeness  = data.completeness || [];
        const syncedAt      = data.reference_synced_at || null;
        const refSource     = data.reference_source || 'manual';
        if (completeness.length) {
            const mColors = { 'A': 'primary', 'HK': 'danger', 'US': 'success' };
            const pctBar  = pct => {
                const c = pct >= 80 ? 'success' : pct >= 50 ? 'warning' : 'danger';
                return `<div class="progress mt-1" style="height:7px"><div class="progress-bar bg-${c}" style="width:${Math.min(pct,100)}%"></div></div>`;
            };
            const pctBadge = pct => pct == null
                ? '<span class="text-muted small">待同步</span>'
                : `<span class="fw-semibold ${pct>=80?'text-success':pct>=50?'text-warning':'text-danger'}">${pct}%</span>`;

            const hasRef = completeness.some(c => c.reference_count > 0);
            let rows = completeness.map(c => {
                const badge = `<span class="badge bg-${mColors[c.market]||'secondary'}">${c.market_name}</span>`;
                const l1Cell = hasRef
                    ? (c.reference_count > 0
                        ? `<div class="d-flex justify-content-between small"><span>${c.total_stocks.toLocaleString()} / ${c.reference_count.toLocaleString()} 只</span>${pctBadge(c.list_coverage)}</div>${pctBar(c.list_coverage)}`
                        : `<span class="text-muted small">—</span>`)
                    : `<span class="text-muted small">点击「从数据源同步」获取</span>`;
                const l2Pct = c.kline_coverage;
                const l2Cell = `<div class="d-flex justify-content-between small"><span>${c.stocks_with_klines.toLocaleString()} / ${c.total_stocks.toLocaleString()} 只</span>${pctBadge(l2Pct)}</div>${pctBar(l2Pct)}${c.stocks_no_klines>0?`<div class="text-muted mt-1" style="font-size:.75rem">缺失 ${c.stocks_no_klines.toLocaleString()} 只</div>`:''}`;
                return `<tr><td>${badge}</td><td>${l1Cell}</td><td>${l2Cell}</td></tr>`;
            }).join('');

            document.getElementById('dash-completeness').innerHTML = `
                <div class="table-responsive">
                    <table class="table table-sm align-middle mb-0" style="font-size:.85rem">
                        <thead class="table-light"><tr>
                            <th style="width:70px">市场</th>
                            <th>第一层：股票列表完整度<small class="text-muted fw-normal ms-1">（本地库存 / 数据源总数）</small></th>
                            <th>第二层：K线数据完整度<small class="text-muted fw-normal ms-1">（有K线 / 本地库存）</small></th>
                        </tr></thead>
                        <tbody>${rows}</tbody>
                    </table>
                </div>`;
            document.getElementById('dash-completeness-card').style.display = '';

            // 同步时间提示
            const syncInfo = document.getElementById('dash-ref-sync-info');
            if (syncInfo) {
                syncInfo.textContent = syncedAt
                    ? `参考数据来源：数据源实时（${syncedAt}）`
                    : '参考总数尚未同步，第一层暂不可用';
            }
            // 管理员才显示同步按钮
            const syncBtn = document.getElementById('dash-sync-btn');
            if (syncBtn && currentUser && currentUser.role === 'admin') syncBtn.style.display = '';
        }

        // ── 更新提示 ──
        const hasAdmin = currentUser && (currentUser.role === 'admin' || (currentUser.permissions||[]).includes('data_management'));
        let tipHtml = '';
        if (!latestDate) {
            tipHtml = `<div class="alert alert-danger py-2 d-flex align-items-center gap-3 mb-0">
                <span>🔴 <strong>暂无数据</strong>，请先更新股票数据</span>
                ${hasAdmin ? '<button class="btn btn-sm btn-danger ms-auto" onclick="showTab(\'config\', null)">立即更新 →</button>' : ''}
            </div>`;
        } else if (fi.days > 180) {
            tipHtml = `<div class="alert alert-danger py-2 d-flex align-items-center gap-3 mb-0">
                <span>🔴 数据距今已超 <strong>${Math.floor(fi.days)}</strong> 天，建议尽快更新</span>
                ${hasAdmin ? '<button class="btn btn-sm btn-danger ms-auto" onclick="showTab(\'config\', null)">立即更新 →</button>' : ''}
            </div>`;
        } else if (fi.days > 45) {
            tipHtml = `<div class="alert alert-warning py-2 d-flex align-items-center gap-3 mb-0">
                <span>🟡 数据距今已 <strong>${Math.floor(fi.days)}</strong> 天，建议更新</span>
                ${hasAdmin ? '<button class="btn btn-sm btn-warning ms-auto" onclick="showTab(\'config\', null)">前往更新 →</button>' : ''}
            </div>`;
        } else {
            tipHtml = `<div class="alert alert-success py-2 mb-0">🟢 数据已是最新，上次更新：<strong>${_fmtDate(latestDate)}</strong></div>`;
        }
        document.getElementById('dash-update-tip').innerHTML = tipHtml;

        // ── 市场覆盖 ──
        const mColors = {'A股':'primary','港股':'danger','美股':'success'};
        let marketsHtml = '';
        if (markets.length) {
            marketsHtml = markets.map(m => {
                const color  = mColors[m.market_name] || 'secondary';
                const fi2    = _freshnessInfo(m.latest_date);
                const spanY  = (m.earliest_date && m.latest_date)
                    ? parseInt(m.latest_date.slice(0,4)) - parseInt(m.earliest_date.slice(0,4)) : 0;
                const spanStr = spanY > 0
                    ? `${m.earliest_date.slice(0,4)} ~ ${m.latest_date.slice(0,4)}（${spanY}年）`
                    : _fmtDate(m.latest_date);
                return `<div class="d-flex align-items-center gap-3 py-2 border-bottom">
                    <span class="badge bg-${color} fs-6" style="width:44px">${m.market_name}</span>
                    <div class="flex-grow-1">
                        <div class="d-flex justify-content-between small">
                            <span><strong>${m.stock_count.toLocaleString()}</strong> 只 &nbsp;·&nbsp; ${m.data_count ? m.data_count.toLocaleString() + ' 条' : '—'}</span>
                            <span class="${fi2.cls}">${fi2.icon} ${_fmtDate(m.latest_date)}</span>
                        </div>
                        <div class="text-muted" style="font-size:.78rem">${spanStr}</div>
                    </div>
                </div>`;
            }).join('');
        } else {
            marketsHtml = '<div class="text-muted small">暂无市场数据</div>';
        }
        document.getElementById('dash-markets').innerHTML = marketsHtml;

        // ── 数据源表格 ──
        let srcHtml = '';
        if (sources.length) {
            const mBadge = {'A股':'primary','港股':'danger','美股':'success'};
            srcHtml = `<div class="table-responsive"><table class="table table-sm table-hover align-middle mb-0" style="font-size:.82rem">
                <thead class="table-light"><tr>
                    <th>数据源</th><th>市场</th><th class="text-end">K线数</th><th>股票覆盖</th><th>时间跨度</th>
                </tr></thead><tbody>`;
            sources.forEach(src => {
                const empty = src.data_count === 0;
                const pct   = totalStocks > 0 ? Math.min(100, Math.round(src.stock_count / totalStocks * 100)) : 0;
                const bar   = empty ? 'bg-secondary' : (pct>=80?'bg-success':pct>=30?'bg-warning':'bg-danger');
                const mkts  = (src.markets||[]).map(m=>`<span class="badge bg-${mBadge[m]||'secondary'} me-1" style="font-size:.7rem">${m}</span>`).join('');
                let span = '<span class="text-muted">—</span>';
                if (!empty && src.earliest_date && src.latest_date) {
                    const yrs = parseInt(src.latest_date.slice(0,4)) - parseInt(src.earliest_date.slice(0,4));
                    const fi3 = _freshnessInfo(src.latest_date);
                    span = `${src.earliest_date.slice(0,4)}~<span class="${fi3.cls}">${src.latest_date.slice(0,4)}</span>${yrs>0?` <span class="text-muted">(${yrs}年)</span>`:''}`;
                }
                srcHtml += `<tr class="${empty?'text-muted':''}">
                    <td><span class="badge bg-${empty?'secondary':'info'}">${src.data_source}</span>${empty?'<br><span class="text-muted" style="font-size:.75rem">暂无数据</span>':''}</td>
                    <td>${mkts||'—'}</td>
                    <td class="text-end">${empty?'0':src.data_count.toLocaleString()}</td>
                    <td>${empty?'<span class="text-muted">—</span>':`<div class="d-flex align-items-center gap-1"><div class="progress flex-grow-1" style="height:6px;min-width:50px"><div class="progress-bar ${bar}" style="width:${pct}%"></div></div><small>${pct}%</small></div>`}</td>
                    <td>${span}</td>
                </tr>`;
            });
            srcHtml += '</tbody></table></div>';
        } else {
            srcHtml = '<div class="text-muted small p-3">暂无数据源信息</div>';
        }
        document.getElementById('dash-sources').innerHTML = srcHtml;
        document.getElementById('dash-detail-row').style.display = '';

    } catch (error) {
        console.error('loadDataStatus error:', error);
    }
}

// 加载行业分类状态
async function loadDashboardIndustryStats() {
    const card = document.getElementById('dash-industry-card');
    const el   = document.getElementById('dash-industry');
    if (!card || !el) return;
    try {
        const [swRes, citicsRes] = await Promise.all([
            fetch('/api/industries?industry_type=sw&market=A'),
            fetch('/api/industries?industry_type=citics&market=A')
        ]);
        const sw     = await swRes.json();
        const citics = await citicsRes.json();
        const swCnt     = sw.success     ? sw.data.length     : 0;
        const citicsCnt = citics.success ? citics.data.length : 0;
        if (swCnt === 0 && citicsCnt === 0) {
            el.innerHTML = '<span class="text-muted small">暂无行业分类数据，请前往 <b>系统配置→更新行业分类</b> 进行更新。</span>';
        } else {
            el.innerHTML = `<div class="d-flex flex-wrap gap-4 align-items-center small">
                <span><i class="bi bi-diagram-3 text-primary me-1"></i><strong>申万行业</strong>：${swCnt > 0 ? `<span class="text-success fw-semibold">${swCnt} 个</span>` : '<span class="text-muted">暂无</span>'}</span>
                <span><i class="bi bi-diagram-3 text-secondary me-1"></i><strong>中信行业</strong>：${citicsCnt > 0 ? `<span class="text-success fw-semibold">${citicsCnt} 个</span>` : '<span class="text-muted">暂无</span>'}</span>
                <span class="text-muted">（仅统计A股）</span>
            </div>`;
        }
        card.style.display = '';
    } catch(e) {
        console.error('loadDashboardIndustryStats error:', e);
    }
}

// 测试更新数据（仅管理员/数据管理员）
async function testUpdateData() {
    if (!currentUser) {
        alert('请先登录');
        return;
    }
    if (currentUser.role !== 'admin' && !currentUser.permissions?.includes('data_management')) {
        alert('需要管理员或数据管理权限');
        return;
    }

    const marketRadio = document.querySelector('input[name="updateMarket"]:checked');
    const market = marketRadio ? marketRadio.value : '';
    const marketNameMap = {'A': 'A股', 'HK': '港股', 'US': '美股'};
    const marketName = marketNameMap[market] || '全部市场';

    if (!confirm(`测试更新将更新${marketName}前10条股票的数据，用于快速测试功能。\n\n确定要继续吗？`)) {
        return;
    }

    const progressContainer = document.getElementById('update-progress-container');
    if (progressContainer) {
        progressContainer.style.display = 'block';
        document.getElementById('update-progress-bar').style.width = '0%';
        document.getElementById('update-progress-message').textContent = '正在启动测试更新...';
    }

    setUpdateButtonsDisabled(true);
    try {
        const response = await fetch('/api/data/update', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'include',
            body: JSON.stringify({
                is_test: true,
                test_limit: 10,
                market: market || null
            })
        });
        const result = await response.json();

        if (result.success) {
            if (result.already_running) {
                alert('已有更新任务正在运行，请查看下方进度');
            }
            if (progressContainer) progressContainer.style.display = 'block';
            startProgressSSE();
        } else {
            setUpdateButtonsDisabled(false);
            const errorMsg = result.message || '测试更新失败';
            alert('测试更新失败: ' + errorMsg);
            if (progressContainer) progressContainer.style.display = 'none';
        }
    } catch (error) {
        console.error('Test update error:', error);
        setUpdateButtonsDisabled(false);
        alert('测试更新失败: ' + error.message);
        if (progressContainer) progressContainer.style.display = 'none';
    }
}

// 更新行业分类（仅管理员/数据管理员）
async function updateIndustryClassification() {
    if (!currentUser || (currentUser.role !== 'admin' && !currentUser.permissions?.includes('data_management'))) {
        alert('需要管理员或数据管理权限');
        return;
    }
    const marketRadio = document.querySelector('input[name="updateMarket"]:checked');
    const market = marketRadio ? marketRadio.value : '';

    setUpdateButtonsDisabled(true);
    try {
        const response = await fetch('/api/data/update-industry', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'include',
            body: JSON.stringify({ market: market || null })
        });
        const result = await response.json();
        if (result.success) {
            if (result.already_running) {
                alert('已有更新任务正在运行，请查看下方进度');
            }
            document.getElementById('update-progress-container').style.display = 'block';
            document.getElementById('update-progress-bar').style.width = '0%';
            document.getElementById('update-progress-message').textContent = result.message;
            startProgressSSE();
        } else {
            setUpdateButtonsDisabled(false);
            alert('启动失败: ' + (result.message || result.detail || '未知错误'));
        }
    } catch (error) {
        setUpdateButtonsDisabled(false);
        alert('请求失败: ' + error.message);
    }
}

// 更新数据（仅管理员/数据管理员）
async function updateData(mode) {
    if (!currentUser || (currentUser.role !== 'admin' && !currentUser.permissions?.includes('data_management'))) {
        alert('需要管理员或数据管理权限');
        return;
    }
    try {
        const marketRadio = document.querySelector('input[name="updateMarket"]:checked');
        const market = marketRadio ? marketRadio.value : '';
        const marketName = ({'A': 'A股', 'HK': '港股', 'US': '美股'})[market] || '全部市场';

        if (mode === 'rebuild') {
            if (!confirm(`⚠️ 全量重建将删除【${marketName}】当前数据源的所有历史数据，然后重新获取。\n\n此操作不可恢复，确定要继续吗？`)) {
                return;
            }
        } else {
            if (!confirm(`即将对【${marketName}】执行增量更新，只补充缺失数据，耗时可能较长。\n\n确定要继续吗？`)) {
                return;
            }
        }

        // 增量更新时，检查是否有断点可继续
        let resume_checkpoint = false;
        if (mode === 'incremental') {
            try {
                const cpRes = await fetch('/api/data/checkpoint', { credentials: 'include' });
                const cpData = await cpRes.json();
                if (cpData.checkpoint && cpData.checkpoint.market === (market || null)) {
                    const cp = cpData.checkpoint;
                    const done = cp.completed_codes ? cp.completed_codes.length : 0;
                    const total = cp.total || '?';
                    const saved = cp.saved_at || '';
                    const choice = confirm(
                        `检测到上次未完成的更新断点\n` +
                        `已完成：${done}/${total} 只股票（${saved}）\n\n` +
                        `点击「确定」从断点继续\n点击「取消」从头重新开始`
                    );
                    if (choice) {
                        resume_checkpoint = true;
                    } else {
                        await fetch('/api/data/checkpoint', { method: 'DELETE', credentials: 'include' });
                    }
                }
            } catch (e) { /* 忽略断点查询失败 */ }
        }

        setUpdateButtonsDisabled(true);
        const response = await fetch('/api/data/update', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'include',
            body: JSON.stringify({
                update_mode: mode,
                market: market || null,
                resume_checkpoint: resume_checkpoint,
            })
        });
        const result = await response.json();

        if (result.success) {
            if (result.already_running) {
                alert('已有更新任务正在运行，请查看下方进度');
            }
            document.getElementById('update-progress-container').style.display = 'block';
            startProgressSSE();
        } else {
            setUpdateButtonsDisabled(false);
            alert(result.message || '更新失败');
        }
    } catch (error) {
        console.error('Error updating data:', error);
        setUpdateButtonsDisabled(false);
        alert('更新失败: ' + error.message);
    }
}

// 启动SSE进度监听
function startProgressSSE() {
    if (progressEventSource) {
        progressEventSource.close();
        progressEventSource = null;
    }

    // 清空日志区域
    const logEl = document.getElementById('update-progress-log');
    if (logEl) { logEl.innerHTML = ''; logEl.style.display = 'none'; }

    progressEventSource = new EventSource('/api/data/progress');
    progressEventSource.onmessage = function(event) {
        try {
            const data = JSON.parse(event.data);
            const progress = (data.current / data.total) * 100;
            document.getElementById('update-progress-bar').style.width = progress + '%';
            document.getElementById('update-progress-bar').setAttribute('aria-valuenow', progress);
            document.getElementById('update-progress-message').textContent = data.message;

            // 追加日志条目
            if (data.message && logEl) {
                logEl.style.display = 'block';
                const line = document.createElement('div');
                const time = new Date().toLocaleTimeString('zh-CN', {hour12: false});
                line.textContent = `[${time}] ${data.message}`;
                logEl.appendChild(line);
                logEl.scrollTop = logEl.scrollHeight;
            }

            // 同步暂停/继续按钮状态
            if (data.is_running) {
                const pauseBtn = document.getElementById('btn-pause-update');
                const resumeBtn = document.getElementById('btn-resume-update');
                if (data.paused) {
                    if (pauseBtn) pauseBtn.style.display = 'none';
                    if (resumeBtn) resumeBtn.style.display = '';
                } else {
                    if (pauseBtn) pauseBtn.style.display = '';
                    if (resumeBtn) resumeBtn.style.display = 'none';
                }
            }

            if (!data.is_running) {
                progressEventSource.close();
                progressEventSource = null;
                setUpdateButtonsDisabled(false);
                const bar = document.getElementById('update-progress-bar');
                const isError = data.message && (data.message.includes('失败') || data.message.includes('错误') || data.message.includes('Error'));
                bar.className = isError ? 'progress-bar bg-danger' : 'progress-bar bg-success';
                bar.style.width = '100%';
                loadDataStatus();
                setTimeout(() => {
                    document.getElementById('update-progress-container').style.display = 'none';
                    bar.className = 'progress-bar progress-bar-striped progress-bar-animated';
                    bar.style.width = '0%';
                    if (logEl) { logEl.innerHTML = ''; logEl.style.display = 'none'; }
                }, 5000);
            }
        } catch (e) {
            console.error('SSE parse error:', e);
        }
    };
    progressEventSource.onerror = function(e) {
        if (!progressEventSource) return;  // onmessage 已处理
        console.error('SSE error:', e);
        progressEventSource.close();
        progressEventSource = null;
        setUpdateButtonsDisabled(false);
        const msgEl = document.getElementById('update-progress-message');
        if (msgEl) msgEl.textContent = '进度连接已断开，更新可能仍在后台运行，请稍后刷新页面查看结果';
        const bar = document.getElementById('update-progress-bar');
        if (bar) bar.className = 'progress-bar bg-warning';
    };
}

// 检查并显示进度（页面加载时调用）
function checkAndShowProgress() {
    // 连接SSE；第一个事件会立即反映当前状态
    startProgressSSE();
}

// 单股分析
async function analyzeStock() {
    const code = document.getElementById('stock-code').value.trim();
    const market = document.getElementById('stock-market')?.value || '';
    const month = parseInt(document.getElementById('stock-month').value);
    const startYear = parseInt(document.getElementById('stock-start-year').value);
    const endYear = parseInt(document.getElementById('stock-end-year').value);

    if (!code) {
        alert('请输入股票代码');
        return;
    }
    if (!validateYearRange('stock-start-year', 'stock-end-year')) return;

    const resultDiv = document.getElementById('stock-result');
    resultDiv.innerHTML = '<div class="loading">查询中...</div>';

    try {
        const excludeRelisting = document.getElementById('stock-exclude-relisting')?.checked || false;
        const requestBody = {
            code: code,
            month: month,
            start_year: startYear,
            end_year: endYear,
            exclude_relisting: excludeRelisting
        };

        const response = await fetch('/api/stock/statistics', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(requestBody)
        });
        
        const result = await response.json();
        if (result.success) {
            displayStockResult(result.data);
        } else {
            resultDiv.innerHTML = '<div class="alert alert-danger">查询失败: ' + (result.message || '未知错误') + '</div>';
        }
    } catch (error) {
        resultDiv.innerHTML = '<div class="alert alert-danger">查询失败: ' + error.message + '</div>';
    }
}

// 显示股票分析结果
function displayStockResult(data) {
    const resultDiv = document.getElementById('stock-result');
    
    if (data.total_count === 0) {
        let message = data.message || '该股票在指定月份和年份范围内没有数据';
        let suggestion = '';
        
        if (data.market === 'HK' || data.market === 'US') {
            suggestion = '<br><small class="text-muted">提示：如果是港股或美股，请确保已在"数据管理"页面更新了该市场的数据。</small>';
        } else {
            suggestion = '<br><small class="text-muted">提示：请检查年份范围是否正确，或先在"数据管理"页面更新数据。</small>';
        }
        
        resultDiv.innerHTML = `<div class="alert alert-warning">
            <strong>无数据</strong><br>
            ${message}
            ${suggestion}
        </div>`;
        return;
    }
    
    // 获取数据源信息
    const dataSource = data.data_source || '未知';
    const dataSourceBadge = `<span class="badge bg-secondary ms-2">数据源: ${dataSource}</span>`;
    
    // 获取市场信息（如果有）
    const market = data.market || '';
    const currency = data.currency || '';
    let marketBadge = '';
    if (market === 'A') {
        marketBadge = '<span class="badge bg-primary ms-2">A股</span>';
    } else if (market === 'HK') {
        marketBadge = '<span class="badge bg-success ms-2">港股</span>';
    } else if (market === 'US') {
        marketBadge = '<span class="badge bg-info ms-2">美股</span>';
    }
    const currencyBadge = currency ? `<span class="badge bg-light text-dark ms-2">${currency}</span>` : '';
    
    const html = `
        <h6>${data.name} (${data.symbol}) ${marketBadge}${currencyBadge} - ${data.month}月统计 ${dataSourceBadge}</h6>
        <div class="row">
            <div class="col-md-2">
                <div class="stat-card">
                    <div class="stat-label">总交易次数</div>
                    <div class="stat-value">${data.total_count}</div>
                </div>
            </div>
            <div class="col-md-2">
                <div class="stat-card">
                    <div class="stat-label">上涨次数</div>
                    <div class="stat-value text-success">${data.up_count}</div>
                </div>
            </div>
            <div class="col-md-2">
                <div class="stat-card">
                    <div class="stat-label">下跌次数</div>
                    <div class="stat-value text-danger">${data.down_count}</div>
                </div>
            </div>
            <div class="col-md-2">
                <div class="stat-card">
                    <div class="stat-label">平盘次数</div>
                    <div class="stat-value text-secondary">${data.flat_count ?? 0}</div>
                </div>
            </div>
            <div class="col-md-2">
                <div class="stat-card">
                    <div class="stat-label">上涨概率</div>
                    <div class="stat-value text-success">${data.up_probability}%</div>
                </div>
            </div>
            <div class="col-md-2">
                <div class="stat-card">
                    <div class="stat-label">平盘概率</div>
                    <div class="stat-value text-secondary">${data.flat_probability ?? 0}%</div>
                </div>
            </div>
        </div>
        <div class="row mt-3">
            <div class="col-md-6">
                <div class="card">
                    <div class="card-body">
                        <h6>平均涨幅</h6>
                        <p class="text-success" style="font-size: 20px; font-weight: bold;">${data.avg_up_pct}%</p>
                    </div>
                </div>
            </div>
            <div class="col-md-6">
                <div class="card">
                    <div class="card-body">
                        <h6>平均跌幅</h6>
                        <p class="text-danger" style="font-size: 20px; font-weight: bold;">${data.avg_down_pct}%</p>
                    </div>
                </div>
            </div>
        </div>
        <div class="mt-3">
            <button class="btn btn-success" onclick="exportStockStatistics()">
                <i class="bi bi-file-earmark-excel"></i> 导出Excel
            </button>
        </div>
    `;
    
    resultDiv.innerHTML = html;
}

// 导出单只股票统计
async function exportStockStatistics() {
    if (!requireExportPerm()) return;
    const code = document.getElementById('stock-code').value.trim();
    if (!code) { alert('请输入股票代码'); return; }
    try {
        const filename = await _postDownload('/api/export/stock-statistics', {
            code, month: parseInt(document.getElementById('stock-month').value),
            start_year: parseInt(document.getElementById('stock-start-year').value),
            end_year: parseInt(document.getElementById('stock-end-year').value)
        }, '正在导出股票统计…');
        showToast('导出成功：' + filename, 'success');
    } catch(e) {
        _hideProgress(false);
        handleExportError(e.message);
    }
}

// 月榜单
async function filterByMonth() {
    if (!validateYearRange('filter-start-year', 'filter-end-year')) return;
    const month = parseInt(document.getElementById('filter-month').value);
    const startYear = parseInt(document.getElementById('filter-start-year').value);
    const endYear = parseInt(document.getElementById('filter-end-year').value);
    const topN = parseInt(document.getElementById('filter-top-n').value);
    const minCount = parseInt(document.getElementById('filter-min-count').value) || 0;
    const market = document.getElementById('filter-market').value;

    const resultDiv = document.getElementById('filter-result');
    resultDiv.innerHTML = '<div class="loading">查询中...</div>';

    try {
        const excludeRelisting = document.getElementById('filter-exclude-relisting')?.checked || false;
        const requestBody = {
            month: month,
            start_year: startYear,
            end_year: endYear,
            top_n: topN,
            min_count: minCount,
            exclude_relisting: excludeRelisting
        };
        if (market) {
            requestBody.market = market;
        }

        const response = await fetch('/api/month/filter', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(requestBody)
        });
        
        const result = await response.json();
        if (result.success) {
            displayFilterResult(result.data, month, result.data_source, minCount);
        } else {
            resultDiv.innerHTML = '<div class="alert alert-danger">查询失败: ' + (result.message || '未知错误') + '</div>';
        }
    } catch (error) {
        resultDiv.innerHTML = '<div class="alert alert-danger">查询失败: ' + error.message + '</div>';
    }
}

// 显示月份筛选结果
function displayFilterResult(data, month, dataSource, minCount) {
    const resultDiv = document.getElementById('filter-result');
    
    if (data.length === 0) {
        resultDiv.innerHTML = '<div class="alert alert-warning">没有找到数据</div>';
        return;
    }
    
    const dataSourceBadge = dataSource ? `<span class="badge bg-secondary ms-2">数据源: ${dataSource}</span>` : '';
    const minCountBadge = minCount > 0 ? `<span class="badge bg-info ms-2">最小涨跌次数: ${minCount}</span>` : '';
    let html = `<h6>${month}月上涨概率最高的前${data.length}支股票 ${dataSourceBadge} ${minCountBadge}</h6>`;
    html += '<div class="table-responsive"><table class="table table-striped table-hover">';
    html += '<thead><tr><th>排名</th><th>股票代码</th><th>股票名称</th><th>上涨概率</th><th>上涨次数</th><th>下跌次数</th><th>平盘次数</th><th>平盘概率</th><th>平均涨幅</th><th>平均跌幅</th><th>数据源</th></tr></thead><tbody>';

    data.forEach((item, index) => {
        const itemDataSource = item.data_source || dataSource || '未知';
        html += `<tr>
            <td>${index + 1}</td>
            <td>${item.symbol}</td>
            <td>${item.name}</td>
            <td><span class="badge bg-success">${item.up_probability}%</span></td>
            <td>${item.up_count}</td>
            <td>${item.down_count}</td>
            <td class="text-secondary">${item.flat_count ?? 0}</td>
            <td class="text-secondary">${item.flat_probability ?? 0}%</td>
            <td class="text-success">${item.avg_up_pct}%</td>
            <td class="text-danger">${item.avg_down_pct}%</td>
            <td><span class="badge bg-info">${itemDataSource}</span></td>
        </tr>`;
    });

    html += '</tbody></table></div>';

    // 添加导出按钮
    html += `
        <div class="mt-3">
            <button class="btn btn-success" onclick="exportMonthFilter()">
                <i class="bi bi-file-earmark-excel"></i> 导出Excel
            </button>
        </div>
    `;
    
    resultDiv.innerHTML = html;
    makeSortable(resultDiv.querySelector('table'));
}

// 导出月榜单
async function exportMonthFilter() {
    if (!requireExportPerm()) return;
    const market = document.getElementById('filter-market').value;
    const body = {
        month: parseInt(document.getElementById('filter-month').value),
        start_year: parseInt(document.getElementById('filter-start-year').value),
        end_year: parseInt(document.getElementById('filter-end-year').value),
        top_n: parseInt(document.getElementById('filter-top-n').value),
        min_count: parseInt(document.getElementById('filter-min-count').value) || 0
    };
    if (market) body.market = market;
    try {
        const filename = await _postDownload('/api/export/month-filter', body, '正在导出月榜单…');
        showToast('导出成功：' + filename, 'success');
    } catch(e) {
        _hideProgress(false);
        handleExportError(e.message);
    }
}

// 行业分析
async function analyzeIndustry() {
    if (!validateYearRange('industry-start-year', 'industry-end-year')) return;
    const industryType = document.getElementById('industry-type').value;
    const month = parseInt(document.getElementById('industry-month').value);
    const startYear = parseInt(document.getElementById('industry-start-year').value);
    const endYear = parseInt(document.getElementById('industry-end-year').value);
    const market = document.getElementById('industry-market').value;

    const resultDiv = document.getElementById('industry-result');
    resultDiv.innerHTML = '<div class="loading">查询中...</div>';

    try {
        const excludeRelisting = document.getElementById('industry-exclude-relisting')?.checked || false;
        const requestBody = {
            month: month,
            start_year: startYear,
            end_year: endYear,
            industry_type: industryType,
            exclude_relisting: excludeRelisting
        };
        if (market) {
            requestBody.market = market;
        }

        const response = await fetch('/api/industry/statistics', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(requestBody)
        });
        
        const result = await response.json();
        if (result.success) {
            displayIndustryResult(result.data, month, industryType, result.data_source);
            // 更新行业选择下拉框
            updateIndustrySelect(result.data);
        } else {
            resultDiv.innerHTML = '<div class="alert alert-danger">查询失败: ' + (result.message || '未知错误') + '</div>';
        }
    } catch (error) {
        resultDiv.innerHTML = '<div class="alert alert-danger">查询失败: ' + error.message + '</div>';
    }
}

// 显示行业分析结果
function displayIndustryResult(data, month, industryType, dataSource) {
    const resultDiv = document.getElementById('industry-result');
    
    if (data.length === 0) {
        resultDiv.innerHTML = '<div class="alert alert-warning">没有找到数据</div>';
        return;
    }
    
    const typeName = industryType === 'sw' ? '申万' : '中信';
    const dataSourceBadge = dataSource ? `<span class="badge bg-secondary ms-2">数据源: ${dataSource}</span>` : '';
    let html = `<h6>${typeName}行业分类 - ${month}月上涨概率统计 ${dataSourceBadge}</h6>`;
    html += '<div class="table-responsive"><table class="table table-striped table-hover">';
    html += '<thead><tr><th>排名</th><th>行业名称</th><th>股票数量</th><th>上涨概率</th><th>上涨次数</th><th>下跌次数</th><th>平盘次数</th><th>平盘概率</th><th>平均涨幅</th><th>平均跌幅</th><th>数据源</th></tr></thead><tbody>';

    data.forEach((item, index) => {
        const itemDataSource = item.data_source || dataSource || '未知';
        html += `<tr>
            <td>${index + 1}</td>
            <td>${item.industry_name}</td>
            <td>${item.stock_count}</td>
            <td><span class="badge bg-success">${item.up_probability}%</span></td>
            <td>${item.up_count}</td>
            <td>${item.down_count}</td>
            <td class="text-secondary">${item.flat_count ?? 0}</td>
            <td class="text-secondary">${item.flat_probability ?? 0}%</td>
            <td class="text-success">${item.avg_up_pct}%</td>
            <td class="text-danger">${item.avg_down_pct}%</td>
            <td><span class="badge bg-info">${itemDataSource}</span></td>
        </tr>`;
    });
    
    html += '</tbody></table></div>';
    
    // 添加导出按钮
    html += `
        <div class="mt-3">
            <button class="btn btn-success" onclick="exportIndustryStatistics()">
                <i class="bi bi-file-earmark-excel"></i> 导出Excel
            </button>
        </div>
    `;
    
    resultDiv.innerHTML = html;
    makeSortable(resultDiv.querySelector('table'));
}

// 导出行业统计
async function exportIndustryStatistics() {
    if (!requireExportPerm()) return;
    const market = document.getElementById('industry-market').value;
    const body = {
        month: parseInt(document.getElementById('industry-month').value),
        start_year: parseInt(document.getElementById('industry-start-year').value),
        end_year: parseInt(document.getElementById('industry-end-year').value),
        industry_type: document.getElementById('industry-type').value
    };
    if (market) body.market = market;
    try {
        const filename = await _postDownload('/api/export/industry-statistics', body, '正在导出行业统计…');
        showToast('导出成功：' + filename, 'success');
    } catch(e) {
        _hideProgress(false);
        handleExportError(e.message);
    }
}

// 更新行业选择下拉框
function updateIndustrySelect(industryData) {
    const select = document.getElementById('industry-select');
    select.innerHTML = '<option value="">请选择行业</option>';
    industryData.forEach(item => {
        const option = document.createElement('option');
        option.value = item.industry_name;
        option.textContent = item.industry_name;
        select.appendChild(option);
    });
}

// 获取行业前20支股票
async function getIndustryTopStocks() {
    const industryName = document.getElementById('industry-select').value;
    const industryType = document.getElementById('industry-type').value;
    const month = parseInt(document.getElementById('industry-month').value);
    const startYear = parseInt(document.getElementById('industry-start-year').value);
    const endYear = parseInt(document.getElementById('industry-end-year').value);
    const topN = parseInt(document.getElementById('industry-top-n').value);
    const market = document.getElementById('industry-top-market').value;

    if (!industryName) {
        alert('请先选择行业');
        return;
    }

    const resultDiv = document.getElementById('industry-result');
    resultDiv.innerHTML = '<div class="loading">查询中...</div>';

    try {
        const excludeRelisting = document.getElementById('industry-exclude-relisting')?.checked || false;
        const requestBody = {
            industry_name: industryName,
            month: month,
            start_year: startYear,
            end_year: endYear,
            industry_type: industryType,
            top_n: topN,
            exclude_relisting: excludeRelisting
        };
        if (market) {
            requestBody.market = market;
        }

        const response = await fetch('/api/industry/top-stocks', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(requestBody)
        });
        
        const result = await response.json();
        if (result.success) {
            displayIndustryTopStocks(result.data, industryName, month, result.data_source);
        } else {
            resultDiv.innerHTML = '<div class="alert alert-danger">查询失败: ' + (result.message || '未知错误') + '</div>';
        }
    } catch (error) {
        resultDiv.innerHTML = '<div class="alert alert-danger">查询失败: ' + error.message + '</div>';
    }
}

// 显示行业前20支股票
function displayIndustryTopStocks(data, industryName, month, dataSource) {
    const resultDiv = document.getElementById('industry-result');
    
    if (data.length === 0) {
        resultDiv.innerHTML = '<div class="alert alert-warning">该行业没有找到数据</div>';
        return;
    }
    
    const dataSourceBadge = dataSource ? `<span class="badge bg-secondary ms-2">数据源: ${dataSource}</span>` : '';
    let html = `<h6>${industryName} - ${month}月上涨概率最高的前${data.length}支股票 ${dataSourceBadge}</h6>`;
    html += '<div class="table-responsive"><table class="table table-striped table-hover">';
    html += '<thead><tr><th>排名</th><th>股票代码</th><th>股票名称</th><th>上涨概率</th><th>上涨次数</th><th>下跌次数</th><th>平盘次数</th><th>平盘概率</th><th>平均涨幅</th><th>平均跌幅</th><th>数据源</th></tr></thead><tbody>';

    data.forEach((item, index) => {
        const itemDataSource = item.data_source || dataSource || '未知';
        html += `<tr>
            <td>${index + 1}</td>
            <td>${item.symbol}</td>
            <td>${item.name}</td>
            <td><span class="badge bg-success">${item.up_probability}%</span></td>
            <td>${item.up_count}</td>
            <td>${item.down_count}</td>
            <td class="text-secondary">${item.flat_count ?? 0}</td>
            <td class="text-secondary">${item.flat_probability ?? 0}%</td>
            <td class="text-success">${item.avg_up_pct}%</td>
            <td class="text-danger">${item.avg_down_pct}%</td>
            <td><span class="badge bg-info">${itemDataSource}</span></td>
        </tr>`;
    });

    html += '</tbody></table></div>';

    // 添加导出按钮
    html += `
        <div class="mt-3">
            <button class="btn btn-success" onclick="exportIndustryTopStocks()">
                <i class="bi bi-file-earmark-excel"></i> 导出Excel
            </button>
        </div>
    `;
    
    resultDiv.innerHTML = html;
    makeSortable(resultDiv.querySelector('table'));
}

// 导出行业前20支股票
async function exportIndustryTopStocks() {
    if (!requireExportPerm()) return;
    const industryName = document.getElementById('industry-select').value;
    if (!industryName) { alert('请先选择行业'); return; }
    const market = document.getElementById('industry-top-market').value;
    const body = {
        industry_name: industryName,
        month: parseInt(document.getElementById('industry-month').value),
        start_year: parseInt(document.getElementById('industry-start-year').value),
        end_year: parseInt(document.getElementById('industry-end-year').value),
        industry_type: document.getElementById('industry-type').value,
        top_n: parseInt(document.getElementById('industry-top-n').value)
    };
    if (market) body.market = market;
    try {
        const filename = await _postDownload('/api/export/industry-top-stocks', body, '正在导出行业前N支股票…');
        showToast('导出成功：' + filename, 'success');
    } catch(e) {
        _hideProgress(false);
        handleExportError(e.message);
    }
}

// 加载行业列表
async function loadIndustries() {
    const industryType = document.getElementById('industry-type').value;
    const marketEl = document.getElementById('industry-market');
    const market = marketEl ? marketEl.value : '';
    try {
        const url = market
            ? `/api/industries?industry_type=${industryType}&market=${market}`
            : `/api/industries?industry_type=${industryType}`;
        const response = await fetch(url);
        const result = await response.json();
        if (result.success) {
            const select = document.getElementById('industry-select');
            select.innerHTML = '<option value="">请先查询行业统计</option>';
            result.data.forEach(industry => {
                const option = document.createElement('option');
                option.value = industry;
                option.textContent = industry;
                select.appendChild(option);
            });
        }
    } catch (error) {
        console.error('Error loading industries:', error);
    }
}

// 根据所选数据源显示/隐藏凭证输入框
function onDataSourceChange() {
    const sourceA  = document.getElementById('config-source-a').value;
    const sourceHK = document.getElementById('config-source-hk').value;
    const sourceUS = document.getElementById('config-source-us').value;
    document.getElementById('cred-tushare-a').style.display = sourceA === 'tushare' ? '' : 'none';
    document.getElementById('cred-jqdata').style.display = sourceA === 'jqdata' ? '' : 'none';
    document.getElementById('cred-tushare-hk').style.display = sourceHK === 'tushare' ? '' : 'none';
    document.getElementById('cred-alpha-vantage').style.display = sourceUS === 'alpha_vantage' ? '' : 'none';
}

function onProxyToggle() {
    const enabled = document.getElementById('config-proxy-enabled').checked;
    document.getElementById('proxy-settings').style.display = enabled ? '' : 'none';
}

// 导出股票数据备份
async function klineExport() {
    const market = document.getElementById('kline-export-market')?.value || '';
    const url = '/api/admin/kline-export' + (market ? `?market=${market}` : '');
    try {
        const filename = await _fetchDownload(url, '正在导出K线数据…');
        showToast('导出成功：' + filename, 'success');
    } catch(e) {
        _hideProgress(false);
        handleExportError(e.message);
    }
}

// 选文件时自动识别市场并预选
function klineFileSelected(input) {
    if (!input.files.length) return;
    const name = input.files[0].name.toLowerCase();
    const sel  = document.getElementById('kline-import-market');
    if (!sel) return;
    // 文件名格式：klinedata_A_xxx.db.gz / klinedata_HK_xxx / klinedata_US_xxx / klinedata_all_xxx
    if (/_a[_.]/.test(name) || name.includes('_a_'))      sel.value = 'A';
    else if (/_hk[_.]/.test(name) || name.includes('_hk_')) sel.value = 'HK';
    else if (/_us[_.]/.test(name) || name.includes('_us_')) sel.value = 'US';
    else                                                     sel.value = '';   // 全部市场
}

// 导入还原股票数据
async function klineImport() {
    const fileInput = document.getElementById('kline-import-file');
    const resultEl  = document.getElementById('kline-import-result');
    if (!fileInput || !fileInput.files.length) { alert('请先选择 .db.gz 备份文件'); return; }

    const mode         = document.querySelector('input[name="kline-import-mode"]:checked')?.value || 'merge';
    const importMarket = document.getElementById('kline-import-market')?.value || '';
    const filename     = fileInput.files[0].name;

    // 检测文件名中的市场与当前选择是否一致
    const MARKET_LABELS = { A: 'A股', HK: '港股', US: '美股', '': '全部市场' };
    const nameLower = filename.toLowerCase();
    let fileMarket = '';
    if (/_a[_.]/.test(nameLower) || nameLower.includes('_a_'))       fileMarket = 'A';
    else if (/_hk[_.]/.test(nameLower) || nameLower.includes('_hk_')) fileMarket = 'HK';
    else if (/_us[_.]/.test(nameLower) || nameLower.includes('_us_')) fileMarket = 'US';

    const mismatch = fileMarket !== '' && fileMarket !== importMarket;
    const mismatchHtml = mismatch ? `
        <div style="background:#fef3c7;border:1px solid #f59e0b;border-radius:6px;padding:8px 12px;margin-top:8px;font-size:13px;">
            <i class="bi bi-exclamation-triangle text-warning me-1"></i>
            <b>注意：</b>文件名显示为 <b>${MARKET_LABELS[fileMarket]}</b> 备份，
            但当前选择的是 <b>${MARKET_LABELS[importMarket]}</b>，请确认选择正确。
        </div>` : '';

    const modeLabel   = mode === 'replace'
        ? '<span style="color:#dc2626;font-weight:600;">全量替换（将清空该市场现有K线数据）</span>'
        : '<span style="color:#16a34a;font-weight:600;">合并（保留现有数据，补充缺失）</span>';

    const confirmed = await _confirmDialog({
        title: '确认导入操作',
        body: `
            <table style="font-size:13.5px;border-collapse:separate;border-spacing:0 6px;width:100%">
                <tr><td style="color:#6b7280;width:70px;">文件</td><td><code style="font-size:12px;">${filename}</code></td></tr>
                <tr><td style="color:#6b7280;">目标市场</td><td><b>${MARKET_LABELS[importMarket]}</b></td></tr>
                <tr><td style="color:#6b7280;">导入模式</td><td>${modeLabel}</td></tr>
            </table>
            ${mismatchHtml}
        `,
        confirmText: '确认导入',
        confirmClass: mode === 'replace' ? 'btn-danger' : 'btn-primary',
    });
    if (!confirmed) return;

    resultEl.innerHTML = '';
    let importUrl = `/api/admin/kline-import?mode=${mode}`;
    if (importMarket) importUrl += `&market=${importMarket}`;
    const formData = new FormData();
    formData.append('file', fileInput.files[0]);
    try {
        const result = await _xhrUpload(importUrl, formData, '正在上传K线数据…', '正在导入处理…');
        if (result.success) {
            _hideProgress(true);
            resultEl.innerHTML = `<span class="text-success"><i class="bi bi-check-circle me-1"></i>${result.message}（股票 ${result.stocks} 条，K线 ${result.klines} 条）</span>`;
        } else {
            _hideProgress(false);
            resultEl.innerHTML = `<span class="text-danger"><i class="bi bi-x-circle me-1"></i>${result.message}</span>`;
        }
    } catch(e) {
        _hideProgress(false);
        resultEl.innerHTML = `<span class="text-danger">请求失败: ${e.message}</span>`;
    }
}

// Token 显示/隐藏切换
function toggleTokenVisibility(inputId, btn) {
    const input = document.getElementById(inputId);
    if (!input) return;
    if (input.type === 'password') {
        input.type = 'text';
        btn.innerHTML = '<i class="bi bi-eye-slash"></i>';
    } else {
        input.type = 'password';
        btn.innerHTML = '<i class="bi bi-eye"></i>';
    }
}

// 禁用/启用所有更新按钮，同时切换控制按钮显示
function setUpdateButtonsDisabled(disabled) {
    ['btn-incremental-update', 'btn-rebuild-update', 'btn-test-update', 'btn-industry-update'].forEach(id => {
        const btn = document.getElementById(id);
        if (btn) btn.disabled = disabled;
    });
    const actionBtns = document.getElementById('update-action-buttons');
    const controlBtns = document.getElementById('update-control-buttons');
    const resetRow = document.getElementById('update-reset-row');
    if (disabled) {
        if (actionBtns) actionBtns.style.opacity = '0.5';
        if (controlBtns) { controlBtns.style.display = 'flex'; }
        if (resetRow) resetRow.style.display = 'none';
        // 重置暂停/继续状态
        const pauseBtn = document.getElementById('btn-pause-update');
        const resumeBtn = document.getElementById('btn-resume-update');
        if (pauseBtn) pauseBtn.style.display = '';
        if (resumeBtn) resumeBtn.style.display = 'none';
    } else {
        if (actionBtns) actionBtns.style.opacity = '';
        if (controlBtns) controlBtns.style.display = 'none';
        if (resetRow) resetRow.style.display = '';
    }
}

// 暂停更新
async function pauseUpdate() {
    try {
        const res = await fetch('/api/data/update/pause', { method: 'POST', credentials: 'include' });
        const result = await res.json();
        if (result.success) {
            document.getElementById('btn-pause-update').style.display = 'none';
            document.getElementById('btn-resume-update').style.display = '';
        } else {
            alert(result.message || '暂停失败');
        }
    } catch (e) { alert('暂停失败: ' + e.message); }
}

// 继续更新
async function resumeUpdate() {
    try {
        const res = await fetch('/api/data/update/resume', { method: 'POST', credentials: 'include' });
        const result = await res.json();
        if (result.success) {
            document.getElementById('btn-pause-update').style.display = '';
            document.getElementById('btn-resume-update').style.display = 'none';
        } else {
            alert(result.message || '恢复失败');
        }
    } catch (e) { alert('恢复失败: ' + e.message); }
}

// 强制重置卡住的更新状态
async function resetUpdateState() {
    try {
        const res = await fetch('/api/data/update/reset', { method: 'POST', credentials: 'include' });
        const result = await res.json();
        if (result.success) {
            setUpdateButtonsDisabled(false);
            const container = document.getElementById('update-progress-container');
            if (container) container.style.display = 'none';
            const msgEl = document.getElementById('update-progress-message');
            if (msgEl) msgEl.textContent = '';
            alert('状态已重置，现在可以重新开始更新');
        } else {
            alert('重置失败: ' + (result.message || '未知错误'));
        }
    } catch (e) {
        alert('重置失败: ' + e.message);
    }
}

// 停止更新
async function stopUpdate() {
    if (!confirm('确定要停止更新吗？已处理的数据将保留，断点将被保存供下次继续。')) return;
    try {
        const res = await fetch('/api/data/update/stop', { method: 'POST', credentials: 'include' });
        const result = await res.json();
        if (!result.success) alert(result.message || '停止失败');
    } catch (e) { alert('停止失败: ' + e.message); }
}

// 加载配置（仅管理员）
async function loadConfig() {
    if (!currentUser || currentUser.role !== 'admin') {
        return;
    }
    try {
        const response = await fetch('/api/config', {
            credentials: 'include'
        });
        const result = await response.json();
        if (result.success) {
            const config = result.data;
            const mds = config.market_data_sources || {};
            document.getElementById('config-source-a').value = mds.A || 'akshare';
            document.getElementById('config-source-hk').value = mds.HK || 'yfinance';
            document.getElementById('config-source-us').value = mds.US || 'yfinance';
            document.getElementById('config-tushare-token').value = config.tushare?.token || '';
            document.getElementById('config-jqdata-username').value = config.jqdata?.username || '';
            document.getElementById('config-jqdata-password').value = config.jqdata?.password || '';
            document.getElementById('config-av-apikey').value = config.alpha_vantage?.api_key || '';
            const proxyEnabled = config.proxy?.enabled || false;
            document.getElementById('config-proxy-enabled').checked = proxyEnabled;
            document.getElementById('config-proxy-http').value = config.proxy?.http || '';
            document.getElementById('config-proxy-https').value = config.proxy?.https || '';
            onProxyToggle();
            onDataSourceChange();
        }
    } catch (error) {
        console.error('Error loading config:', error);
    }
}

// 保存配置（仅管理员）
async function saveConfig() {
    if (!currentUser || currentUser.role !== 'admin') {
        alert('需要管理员权限');
        return;
    }
    const sourceA  = document.getElementById('config-source-a').value;
    const sourceHK = document.getElementById('config-source-hk').value;
    const sourceUS = document.getElementById('config-source-us').value;
    const tushareToken = document.getElementById('config-tushare-token').value;
    const jqdataUsername = document.getElementById('config-jqdata-username').value;
    const jqdataPassword = document.getElementById('config-jqdata-password').value;
    const avApiKey = document.getElementById('config-av-apikey').value;
    const proxyEnabled = document.getElementById('config-proxy-enabled').checked;
    const proxyHttp = document.getElementById('config-proxy-http').value.trim();
    const proxyHttps = document.getElementById('config-proxy-https').value.trim();

    try {
        const configData = {
            'data_source': sourceA,
            'market_data_sources': { 'A': sourceA, 'HK': sourceHK, 'US': sourceUS },
            'proxy': { 'enabled': proxyEnabled, 'http': proxyHttp, 'https': proxyHttps }
        };
        if (tushareToken) {
            configData['tushare.token'] = tushareToken;
        }
        if (jqdataUsername) {
            configData['jqdata.username'] = jqdataUsername;
        }
        if (jqdataPassword) {
            configData['jqdata.password'] = jqdataPassword;
        }
        if (avApiKey) {
            configData['alpha_vantage.api_key'] = avApiKey;
        }
        const response = await fetch('/api/config', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            credentials: 'include',
            body: JSON.stringify(configData)
        });
        
        const result = await response.json();
        if (result.success) {
            alert('配置已保存');
            loadConfig();
            loadDataStatus();
        } else {
            alert('保存失败: ' + (result.message || '未知错误'));
        }
    } catch (error) {
        alert('保存失败: ' + error.message);
    }
}

// 加载当前数据源配置信息（数据管理页）
async function loadDataSourceConfig() {
    const el = document.getElementById('data-source-config');
    if (!el) return;
    try {
        const response = await fetch('/api/config', { credentials: 'include' });
        const result = await response.json();
        if (result.success) {
            const mds = result.data.market_data_sources || {};
            el.textContent = `A股: ${mds.A || '-'}  |  港股: ${mds.HK || '-'}  |  美股: ${mds.US || '-'}`;
        }
    } catch (error) {
        console.error('Error loading data source config:', error);
    }
}

// 初始化单股分析类型切换
function initStockAnalysisTypeToggle() {
    const singleMonthRadio = document.getElementById('stock-single-month');
    const multiMonthRadio  = document.getElementById('stock-multi-month');
    const multiMonthLabel  = document.querySelector('label[for="stock-multi-month"]');
    const singleMonthForm  = document.getElementById('stock-single-month-form');
    const multiMonthForm   = document.getElementById('stock-multi-month-form');

    if (!singleMonthRadio || !multiMonthRadio || !singleMonthForm || !multiMonthForm) return;

    // 给「多月统计」标签加版本徽章
    if (multiMonthLabel && !multiMonthLabel.querySelector('.plan-badge-basic')) {
        const badge = document.createElement('span');
        badge.className = 'plan-badge-basic';
        badge.style.cssText = 'font-size:10px;font-weight:600;padding:1px 5px;border-radius:4px;margin-left:6px;background:rgba(22,119,255,0.15);color:#1677ff;vertical-align:middle;';
        badge.textContent = '基础';
        multiMonthLabel.appendChild(badge);
    }

    function hasMultiPerm() {
        return !currentUser || currentUser.role === 'admin' ||
               (currentUser.permissions || []).includes('stock_analysis_multi');
    }
    function showSingle() {
        singleMonthForm.style.display = 'block';
        multiMonthForm.style.display = 'none';
    }
    function showMulti() {
        singleMonthForm.style.display = 'none';
        multiMonthForm.style.display = 'block';
    }

    // 根据当前 radio 状态同步表单显示
    if (multiMonthRadio.checked) {
        if (hasMultiPerm()) { showMulti(); } else { multiMonthRadio.checked = false; singleMonthRadio.checked = true; showSingle(); }
    } else { showSingle(); }

    singleMonthRadio.addEventListener('change', function() { if (this.checked) showSingle(); });
    multiMonthRadio.addEventListener('change', function() {
        if (!this.checked) return;
        if (hasMultiPerm()) {
            showMulti();
        } else {
            // 无权限：回拨到单月，弹升级提示
            this.checked = false;
            singleMonthRadio.checked = true;
            showUpgradeModal('基础版', 'stock_analysis_multi', null, this);
        }
    });
}

// 多月统计查询
async function analyzeStockMultiMonth() {
    const code = document.getElementById('stock-multi-code').value.trim();
    const startYear = parseInt(document.getElementById('stock-multi-start-year').value);
    const endYear = parseInt(document.getElementById('stock-multi-end-year').value);
    // 获取选中的月份
    const monthSelect = document.getElementById('stock-multi-months');
    const selectedMonths = Array.from(monthSelect.selectedOptions).map(opt => parseInt(opt.value));
    // 如果没有选择任何月份，则查询所有月份
    const months = selectedMonths.length > 0 ? selectedMonths : [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];

    if (!code) {
        alert('请输入股票代码');
        return;
    }
    if (!validateYearRange('stock-multi-start-year', 'stock-multi-end-year')) return;

    const resultDiv = document.getElementById('stock-result');
    resultDiv.innerHTML = '<div class="loading">查询中...</div>';

    try {
        const excludeRelisting = document.getElementById('stock-multi-exclude-relisting')?.checked || false;
        const requestBody = {
            code: code,
            months: months,
            start_year: startYear,
            end_year: endYear,
            exclude_relisting: excludeRelisting
        };

        const response = await fetch('/api/stock/multi-month-statistics', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            credentials: 'include',
            body: JSON.stringify(requestBody)
        });
        
        const result = await response.json();
        if (result.success) {
            displayStockMultiMonthResult(result.data);
        } else {
            resultDiv.innerHTML = '<div class="alert alert-danger">查询失败: ' + (result.message || '未知错误') + '</div>';
        }
    } catch (error) {
        resultDiv.innerHTML = '<div class="alert alert-danger">查询失败: ' + error.message + '</div>';
    }
}

// 显示多月统计结果
function displayStockMultiMonthResult(data) {
    const resultDiv = document.getElementById('stock-result');
    
    if (!data || data.length === 0) {
        resultDiv.innerHTML = '<div class="alert alert-warning">该股票在指定月份没有数据</div>';
        return;
    }
    
    const stockName = data[0].name || '';
    const stockSymbol = data[0].symbol || '';
    const dataSource = data[0].data_source || '';
    const dataSourceBadge = dataSource ? `<span class="badge bg-secondary ms-2">数据源: ${dataSource}</span>` : '';
    
    let html = `<h6>${stockName} (${stockSymbol}) - 多月统计 ${dataSourceBadge}</h6>`;
    html += '<div class="table-responsive"><table class="table table-striped table-hover">';
    html += '<thead><tr><th>月份</th><th>总次数</th><th>上涨次数</th><th>下跌次数</th><th>平盘次数</th><th>上涨概率</th><th>下跌概率</th><th>平盘概率</th><th>平均涨幅</th><th>平均跌幅</th></tr></thead><tbody>';

    data.forEach(item => {
        const upProbClass = item.up_probability >= 50 ? 'bg-success' : 'bg-warning';
        const downProbClass = item.down_probability >= 50 ? 'bg-danger' : 'bg-secondary';

        html += `<tr>
            <td><strong>${item.month}月</strong></td>
            <td>${item.total_count}</td>
            <td class="text-success">${item.up_count}</td>
            <td class="text-danger">${item.down_count}</td>
            <td class="text-secondary">${item.flat_count ?? 0}</td>
            <td><span class="badge ${upProbClass}">${item.up_probability}%</span></td>
            <td><span class="badge ${downProbClass}">${item.down_probability}%</span></td>
            <td class="text-secondary">${item.flat_probability ?? 0}%</td>
            <td class="text-success">${item.avg_up_pct}%</td>
            <td class="text-danger">${item.avg_down_pct}%</td>
        </tr>`;
    });
    
    html += '</tbody></table></div>';
    
    // 添加导出按钮
    html += `
        <div class="mt-3">
            <button class="btn btn-success" onclick="exportMultiMonthStatistics()">
                <i class="bi bi-file-earmark-excel"></i> 导出Excel
            </button>
        </div>
    `;
    
    resultDiv.innerHTML = html;
    makeSortable(resultDiv.querySelector('table'));
}

// 导出多月统计
async function exportMultiMonthStatistics() {
    if (!requireExportPerm()) return;
    const code = document.getElementById('stock-multi-code').value.trim();
    if (!code) { alert('请输入股票代码'); return; }
    const monthSelect = document.getElementById('stock-multi-months');
    const selectedMonths = Array.from(monthSelect.selectedOptions).map(opt => parseInt(opt.value));
    try {
        const filename = await _postDownload('/api/export/multi-month-statistics', {
            code,
            months: selectedMonths.length > 0 ? selectedMonths : [1,2,3,4,5,6,7,8,9,10,11,12],
            start_year: parseInt(document.getElementById('stock-multi-start-year').value),
            end_year: parseInt(document.getElementById('stock-multi-end-year').value)
        }, '正在导出多月统计…');
        showToast('导出成功：' + filename, 'success');
    } catch(e) {
        _hideProgress(false);
        handleExportError(e.message);
    }
}

// 数据校对
async function compareDataSources() {
    const code = document.getElementById('compare-code').value.trim();
    const year = document.getElementById('compare-year').value;
    const month = document.getElementById('compare-month').value;
    const date = document.getElementById('compare-date').value.trim();
    
    if (!code) {
        alert('请输入股票代码');
        return;
    }
    
    // 转换股票代码格式（如果用户输入的是6位数字，添加交易所后缀）
    let ts_code = code;
    if (/^\d{6}$/.test(code)) {
        // 根据代码判断交易所
        if (code.startsWith('0') || code.startsWith('3')) {
            ts_code = code + '.SZ';
        } else if (code.startsWith('6') || code.startsWith('9')) {
            ts_code = code + '.SH';
        } else if (code.startsWith('8') || code.startsWith('4')) {
            ts_code = code + '.BJ';
        } else {
            ts_code = code + '.SH'; // 默认上海
        }
    }
    
    const resultDiv = document.getElementById('compare-result');
    resultDiv.innerHTML = '<div class="loading">查询中...</div>';
    
    try {
        const requestBody = {
            ts_code: ts_code
        };
        
        if (date) {
            requestBody.trade_date = date;
        } else {
            if (year) {
                requestBody.year = parseInt(year);
            }
            if (month) {
                requestBody.month = parseInt(month);
            }
        }
        
        const response = await fetch('/api/data/compare-sources', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(requestBody)
        });
        
        const result = await response.json();
        if (result.success) {
            displayCompareResult(result.data, result.message);
        } else {
            resultDiv.innerHTML = '<div class="alert alert-danger">对比失败: ' + (result.message || '未知错误') + '</div>';
        }
    } catch (error) {
        resultDiv.innerHTML = '<div class="alert alert-danger">对比失败: ' + error.message + '</div>';
    }
}

// 显示数据校对结果
function displayCompareResult(data, message) {
    const resultDiv = document.getElementById('compare-result');
    
    if (!data || data.length === 0) {
        resultDiv.innerHTML = `<div class="alert alert-info">${message || '没有找到对比数据，请先使用不同数据源更新数据'}</div>`;
        return;
    }
    
    // 按交易日期分组
    const groupedByDate = {};
    data.forEach(item => {
        const date = item.trade_date;
        if (!groupedByDate[date]) {
            groupedByDate[date] = [];
        }
        groupedByDate[date].push(item);
    });
    
    // 获取所有数据源
    const allSources = [...new Set(data.map(item => item.data_source))];
    
    // 选择第一个数据源作为基准
    const baseSource = allSources[0];
    
    let html = '<div class="table-responsive"><table class="table table-bordered table-hover table-striped">';
    html += '<thead class="table-light"><tr>';
    html += '<th>交易日期</th>';
    html += '<th>数据源</th>';
    html += '<th>开盘价</th>';
    html += '<th>收盘价</th>';
    html += '<th>涨跌幅(%)</th>';
    html += '<th>差异说明</th>';
    html += '</tr></thead><tbody>';
    
    // 按日期排序
    const sortedDates = Object.keys(groupedByDate).sort();
    
    sortedDates.forEach(date => {
        const items = groupedByDate[date];
        const baseItem = items.find(item => item.data_source === baseSource);
        
        items.forEach((item, index) => {
            const isBase = item.data_source === baseSource;
            const rowClass = isBase ? 'table-warning' : '';
            
            let diffText = '';
            if (baseItem && !isBase) {
                const openDiff = ((item.open - baseItem.open) / baseItem.open * 100).toFixed(2);
                const closeDiff = ((item.close - baseItem.close) / baseItem.close * 100).toFixed(2);
                const pctDiff = (item.pct_chg - baseItem.pct_chg).toFixed(2);
                
                diffText = `开盘: ${openDiff > 0 ? '+' : ''}${openDiff}%, `;
                diffText += `收盘: ${closeDiff > 0 ? '+' : ''}${closeDiff}%, `;
                diffText += `涨跌: ${pctDiff > 0 ? '+' : ''}${pctDiff}%`;
            } else if (isBase) {
                diffText = '<span class="badge bg-warning">基准数据源</span>';
            }
            
            html += `<tr class="${rowClass}">`;
            if (index === 0) {
                html += `<td rowspan="${items.length}">${date}</td>`;
            }
            html += `<td><span class="badge bg-info">${item.data_source}</span></td>`;
            html += `<td>${item.open ? parseFloat(item.open).toFixed(2) : '-'}</td>`;
            html += `<td>${item.close ? parseFloat(item.close).toFixed(2) : '-'}</td>`;
            html += `<td>${item.pct_chg ? parseFloat(item.pct_chg).toFixed(2) : '-'}%</td>`;
            html += `<td>${diffText}</td>`;
            html += '</tr>';
        });
    });
    
    html += '</tbody></table></div>';
    
    // 添加导出按钮
    html += `
        <div class="mt-3">
            <button class="btn btn-success" onclick="exportCompareSources()">
                <i class="bi bi-file-earmark-excel"></i> 导出Excel
            </button>
        </div>
    `;
    
    resultDiv.innerHTML = html;
}

// 导出数据校对结果
async function exportCompareSources() {
    if (!requireExportPerm()) return;
    const code = document.getElementById('compare-code').value.trim();
    if (!code) { alert('请输入股票代码'); return; }
    let ts_code = code;
    if (/^\d{6}$/.test(code)) {
        if (code.startsWith('0') || code.startsWith('3')) ts_code = code + '.SZ';
        else if (code.startsWith('6') || code.startsWith('9')) ts_code = code + '.SH';
        else if (code.startsWith('8') || code.startsWith('4')) ts_code = code + '.BJ';
        else ts_code = code + '.SH';
    }
    const date = document.getElementById('compare-date').value.trim();
    const year = document.getElementById('compare-year').value;
    const month = document.getElementById('compare-month').value;
    const body = { ts_code };
    if (date) { body.trade_date = date; } else {
        if (year) body.year = parseInt(year);
        if (month) body.month = parseInt(month);
    }
    try {
        const filename = await _postDownload('/api/export/compare-sources', body, '正在导出数据校对…');
        showToast('导出成功：' + filename, 'success');
    } catch(e) {
        _hideProgress(false);
        handleExportError(e.message);
    }
}

// 股票代码自动补全（每个输入框独立 timeout，避免互相干扰）
const stockAutocompleteTimers = {};
let _stockAutocompleteInitialized = false;

function initStockAutocomplete() {
    if (_stockAutocompleteInitialized) return;  // 防止重复绑定
    _stockAutocompleteInitialized = true;
    const bindings = [
        { inputId: 'stock-code',       dropdownId: 'stock-autocomplete',       marketId: 'stock-market' },
        { inputId: 'stock-multi-code', dropdownId: 'stock-multi-autocomplete', marketId: 'stock-multi-market' },
        { inputId: 'compare-code',     dropdownId: 'compare-autocomplete',     marketId: null },
    ];

    bindings.forEach(({ inputId, dropdownId, marketId }) => {
        const input = document.getElementById(inputId);
        if (!input) return;
        input.addEventListener('input', function(e) {
            handleStockAutocomplete(e.target, dropdownId, marketId);
        });
        input.addEventListener('blur', function() {
            setTimeout(() => {
                const d = document.getElementById(dropdownId);
                if (d) d.style.display = 'none';
            }, 200);
        });
        input.addEventListener('focus', function(e) {
            if (e.target.value.length >= 1) handleStockAutocomplete(e.target, dropdownId, marketId);
        });
        // 切换市场时，若输入框有内容则重新搜索
        if (marketId) {
            const marketEl = document.getElementById(marketId);
            if (marketEl) {
                marketEl.addEventListener('change', function() {
                    const d = document.getElementById(dropdownId);
                    if (d) d.style.display = 'none';
                    if (input.value.trim().length >= 1) {
                        handleStockAutocomplete(input, dropdownId, marketId);
                    }
                });
            }
        }
    });
}

async function handleStockAutocomplete(inputElement, dropdownId, marketId) {
    const keyword = inputElement.value.trim();
    const dropdown = document.getElementById(dropdownId);

    if (!dropdown) return;

    // 每个输入框使用独立 timer，避免互相清除
    if (stockAutocompleteTimers[dropdownId]) clearTimeout(stockAutocompleteTimers[dropdownId]);

    if (keyword.length < 1) {
        dropdown.style.display = 'none';
        return;
    }

    stockAutocompleteTimers[dropdownId] = setTimeout(async () => {
        try {
            const market = marketId ? (document.getElementById(marketId)?.value || '') : '';
            const marketParam = market ? '&market=' + encodeURIComponent(market) : '';
            const response = await fetch('/api/stocks/search?keyword=' + encodeURIComponent(keyword) + '&limit=10' + marketParam);
            const result = await response.json();
            
            if (result.success && result.data && result.data.length > 0) {
                displayStockAutocomplete(result.data, dropdown, inputElement);
            } else {
                dropdown.style.display = 'none';
            }
        } catch (error) {
            console.error('搜索股票失败:', error);
            dropdown.style.display = 'none';
        }
    }, 300);
}

function displayStockAutocomplete(results, dropdown, inputElement) {
    dropdown.innerHTML = '';
    
    // 市场标签映射
    const marketLabels = {
        'A': { label: 'A股', class: 'bg-primary' },
        'HK': { label: '港股', class: 'bg-success' },
        'US': { label: '美股', class: 'bg-info' }
    };
    
    results.forEach(stock => {
        const item = document.createElement('div');
        item.className = 'autocomplete-item';
        const symbol = stock.symbol || stock.ts_code;
        const name = stock.name || '';
        const exchange = stock.exchange || '';
        const market = stock.market || '';
        const currency = stock.currency || '';
        
        // 构建市场标签
        let marketBadge = '';
        if (market && marketLabels[market]) {
            marketBadge = `<span class="badge ${marketLabels[market].class} ms-2">${marketLabels[market].label}</span>`;
        }
        
        // 构建货币标签
        let currencyBadge = '';
        if (currency) {
            currencyBadge = `<small class="text-muted ms-1">(${currency})</small>`;
        }
        
        item.innerHTML = '<strong>' + symbol + '</strong> <span class="text-muted">' + name + '</span>' + marketBadge + currencyBadge + '<small class="text-muted ms-2">' + exchange + '</small>';
        
        item.addEventListener('click', function() {
            inputElement.value = symbol;
            dropdown.style.display = 'none';
            inputElement.dispatchEvent(new Event('input', { bubbles: true }));
        });
        
        item.addEventListener('mouseenter', function() {
            item.style.backgroundColor = '#f0f0f0';
        });
        
        item.addEventListener('mouseleave', function() {
            item.style.backgroundColor = '';
        });
        
        dropdown.appendChild(item);
    });
    
    dropdown.style.display = 'block';
    
    // 获取输入框的位置（相对于其父容器）
    const inputRect = inputElement.getBoundingClientRect();
    const parentRect = inputElement.offsetParent ? inputElement.offsetParent.getBoundingClientRect() : { left: 0, top: 0 };
    
    // 计算相对于父容器的位置
    dropdown.style.top = (inputElement.offsetTop + inputElement.offsetHeight) + 'px';
    dropdown.style.left = inputElement.offsetLeft + 'px';
    dropdown.style.width = inputElement.offsetWidth + 'px';
}

// ========== 认证和权限管理 ==========

// 检查登录状态
async function checkLoginStatus() {
    try {
        const response = await fetch('/api/auth/current-user', {
            credentials: 'include'
        });
        const result = await response.json();

        if (result.success && result.user) {
            currentUser = result.user;

            // 检查账号是否过期
            if (currentUser.expired) {
                alert(currentUser.expired_message || '账号已过期，请联系管理员重新授权');
                showLoginPage();
                return;
            }

            // 确保权限信息存在
            if (!currentUser.permissions) {
                currentUser.permissions = [];
            }

            // 桌面模式：隐藏登录/退出/用户管理相关 UI
            if (result.desktop_mode) {
                const logoutBtn = document.getElementById('logout-btn');
                const userInfo  = document.getElementById('current-user-info');
                const userMgmt  = document.getElementById('user-management-section');
                if (logoutBtn) logoutBtn.style.display = 'none';
                if (userInfo)  userInfo.style.display  = 'none';
                if (userMgmt)  userMgmt.style.display  = 'none';
            }

            showMainContent();
            updateFooterPlan(currentUser);
            updateTrialBanner(currentUser);
            updateUIByRole();
            updateUIByPermissions();
            loadAnnouncementBanners();
            // 点数余额已随登录响应返回，直接更新侧边栏显示
            if (currentUser && currentUser.credits) {
                updateCreditsDisplay(currentUser.credits.total);
            }
            // 每30秒自动刷新侧边栏点数余额
            if (window._creditsRefreshTimer) clearInterval(window._creditsRefreshTimer);
            window._creditsRefreshTimer = setInterval(function() {
                fetch('/api/credits/balance', { credentials: 'include' })
                    .then(r => r.json())
                    .then(d => { if (d.success) { updateCreditsDisplay(d.data.total); if (currentUser) currentUser.credits = d.data; } })
                    .catch(() => {});
            }, 30000);
        } else {
            showLoginPage();
        }
    } catch (error) {
        console.error('检查登录状态失败:', error);
        showLoginPage();
    }
}


// 显示登录页面
function showLoginPage() {
    const loginPage = document.getElementById('login-page');
    if (loginPage) loginPage.style.display = 'flex';
    const mainContent = document.getElementById('main-content');
    if (mainContent) mainContent.style.display = 'none';
    const logoutBtn = document.getElementById('logout-btn');
    if (logoutBtn) logoutBtn.style.display = 'none';
    const userInfo = document.getElementById('current-user-info');
    if (userInfo) userInfo.style.display = 'none';
    currentUser = null;
}

// 显示主内容
function showMainContent() {
    const loginPage = document.getElementById('login-page');
    if (loginPage) loginPage.style.display = 'none';
    document.getElementById('main-content').style.display = 'flex';
    const logoutBtn = document.getElementById('logout-btn');
    if (logoutBtn) logoutBtn.style.display = '';
    const userInfo = document.getElementById('current-user-info');
    if (userInfo) userInfo.style.display = '';
    // 登录成功后显示手机端汉堡按钮
    const mobileBtn = document.getElementById('mobile-fab-btn');
    if (mobileBtn) mobileBtn.style.display = 'flex';
    
    // 显示用户信息
    if (currentUser) {
        const userInfoEl = document.getElementById('current-user-info');
        if (userInfoEl) {
            const adminBadge = currentUser.role === 'admin' ? ' <span class="badge bg-info">管理员</span>' : '';
            userInfoEl.innerHTML = `${currentUser.username}${adminBadge}`;
        }
    }
    
    // 先根据权限更新UI（在加载数据之前）
    updateUIByPermissions();
    
    // 设置默认显示的标签页（根据权限）
    setDefaultTab();
    
    // 初始化主页面功能
    loadDataStatus();
    loadDashboardIndustryStats();
    if (currentUser && (currentUser.role === 'admin' || (currentUser.permissions && currentUser.permissions.includes('data_management')))) {
        checkAndShowProgress();
    }

    loadConfig();
    loadIndustries();
    initStockAnalysisTypeToggle();
    initStockAutocomplete();

    // LOF 仪表盘由 updateUIByPermissions 统一控制
    
    // 如果是管理员，加载用户列表和系统配置
    if (currentUser && currentUser.role === 'admin') {
        loadUsers();
        loadSystemConfig();
        loadPendingGifts();
    }

    // 登录后后台预取常用 tab 数据，用户点击时直接命中缓存
    Object.keys(_prefetchCache).forEach(k => delete _prefetchCache[k]);
    setTimeout(_prefetchAll, 300);

    // 日解锁到期提醒：23:30 后若有日解锁，飘出底部小提示
    _scheduleDailyUnlockReminder();
}

// ===== 日解锁到期提醒 =====
let _dailyUnlockReminderTimer = null;

function _scheduleDailyUnlockReminder() {
    if (_dailyUnlockReminderTimer) clearTimeout(_dailyUnlockReminderTimer);

    const now  = new Date();
    const warn = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 23, 30, 0);
    const msUntilWarn = warn - now;

    if (msUntilWarn <= 0) {
        // 已经过了 23:30，立即检查
        _checkAndShowDailyUnlockReminder();
    } else {
        // 还没到 23:30，定时到点再检查
        _dailyUnlockReminderTimer = setTimeout(_checkAndShowDailyUnlockReminder, msUntilWarn);
    }
}

async function _checkAndShowDailyUnlockReminder() {
    try {
        const res = await fetch('/api/credits/unlocks/today', { credentials: 'include' });
        const d   = await res.json();
        if (!d.success || !d.data || d.data.length === 0) return;

        const names = d.data.map(u => u.name || u.permission_code).join('、');
        _showDailyUnlockToast(names);
    } catch(e) { /* 静默失败，不影响主流程 */ }
}

function _showDailyUnlockToast(featureNames) {
    const old = document.getElementById('daily-unlock-toast');
    if (old) old.remove();

    const toast = document.createElement('div');
    toast.id = 'daily-unlock-toast';
    toast.style.cssText = `
        position:fixed; bottom:24px; left:50%; transform:translateX(-50%);
        background:#1a1a2e; color:#fff; border-radius:10px;
        padding:12px 18px; font-size:13px; z-index:99999;
        box-shadow:0 4px 20px rgba(0,0,0,0.3);
        display:flex; align-items:center; gap:10px;
        max-width:420px; width:92%; animation:slideUpFade .3s ease;
    `;
    toast.innerHTML = `
        <span style="font-size:18px;">⏰</span>
        <div style="flex:1;line-height:1.5;">
            <b style="color:#fbbf24;">今日解锁即将重置</b><br>
            <span style="color:#d1d5db;">${featureNames} 将于 <b style="color:#f87171;">00:00</b> 失效，明日如需继续使用请重新解锁</span>
        </div>
        <span onclick="this.parentElement.remove()" style="cursor:pointer;color:#9ca3af;font-size:18px;padding:0 2px;">×</span>
    `;

    // 注入动画（只注入一次）
    if (!document.getElementById('toast-anim-style')) {
        const s = document.createElement('style');
        s.id = 'toast-anim-style';
        s.textContent = '@keyframes slideUpFade{from{opacity:0;transform:translateX(-50%) translateY(16px)}to{opacity:1;transform:translateX(-50%) translateY(0)}}';
        document.head.appendChild(s);
    }

    document.body.appendChild(toast);

    // 10 秒后自动消失（鼠标悬停时暂停）
    let _autoClose = setTimeout(() => toast.remove(), 10000);
    toast.addEventListener('mouseenter', () => clearTimeout(_autoClose));
    toast.addEventListener('mouseleave', () => { _autoClose = setTimeout(() => toast.remove(), 3000); });
}

// 设置默认显示的标签页
function setDefaultTab() {
    if (!currentUser) return;
    
    const permissions = currentUser.permissions || [];
    const isAdmin = currentUser.role === 'admin';
    
    // 始终优先显示使用指南
    const helpNavItem = document.querySelector('.navbar-nav .nav-item a[onclick*="showTab(\'help\'"]');
    if (helpNavItem) {
        showTab('help', helpNavItem);
        return;
    }

    // 默认标签页优先级（按顺序检查）
    const defaultTabs = [
        'dashboard',           // 首页（优先）
        'stock-analysis',      // 单股分析
        'month-filter',        // 月榜单
        'industry-analysis',   // 行业分析
        'source-compare',      // 数据校对
    ];

    // 权限映射
    const tabPermissionMap = {
        'dashboard': [],       // 无需特殊权限，所有人可见
        'stock-analysis': ['stock_analysis_single', 'stock_analysis_multi'],
        'month-filter': ['month_filter'],
        'industry-analysis': ['industry_statistics', 'industry_top_stocks'],
        'source-compare': ['source_compare'],
    };

    // 如果是管理员，默认显示首页
    if (isAdmin) {
        const navItem = document.querySelector('.navbar-nav .nav-item a[onclick*="showTab(\'dashboard\'"]');
        if (navItem) {
            showTab('dashboard', navItem);
            return;
        }
    }
    
    // 查找第一个有权限的标签页
    for (const tabId of defaultTabs) {
        const requiredPerms = tabPermissionMap[tabId] || [];
        // 无需权限（空数组）或拥有任一所需权限时可访问
        const hasPermission = requiredPerms.length === 0 || requiredPerms.some(perm => permissions.includes(perm));

        if (hasPermission) {
            const navItem = document.querySelector(`.navbar-nav .nav-item a[onclick*="showTab('${tabId}'"]`);
            if (navItem && navItem.closest('.nav-item').style.display !== 'none') {
                showTab(tabId, navItem);
                return;
            }
        }
    }
    
    // 如果都没有权限，显示第一个可见的标签页
    const firstVisibleNavItem = document.querySelector('.navbar-nav .nav-item[style=""] a, .navbar-nav .nav-item:not([style*="none"]) a');
    if (firstVisibleNavItem) {
        const onclick = firstVisibleNavItem.getAttribute('onclick');
        if (onclick) {
            const match = onclick.match(/showTab\('([^']+)'/);
            if (match) {
                showTab(match[1], firstVisibleNavItem);
            }
        }
    }
}

// 根据角色更新UI
function updateUIByRole() {
    const isAdmin = currentUser && currentUser.role === 'admin';
    const adminElements = document.querySelectorAll('.admin-only');
    
    adminElements.forEach(el => {
        el.style.display = isAdmin ? '' : 'none';
    });
    
    // 隐藏普通用户不应该看到的功能
    if (!isAdmin) {
        ['config', 'user-management'].forEach(id => {
            const el = document.getElementById(id);
            if (el) el.style.display = 'none';
        });
    }
}

// 根据用户权限更新UI（所有功能始终显示，无权限时加锁）
function updateUIByPermissions() {
    if (!currentUser) return;

    const permissions = currentUser.permissions || [];
    const isAdmin = currentUser.role === 'admin';

    // 清除所有旧锁图标
    document.querySelectorAll('.nav-lock-icon').forEach(el => el.remove());
    document.querySelectorAll('.navbar-nav .nav-item').forEach(item => {
        item.classList.remove('nav-item-locked');
    });

    // 管理员：显示所有元素直接返回
    if (isAdmin) {
        document.querySelectorAll('.admin-only').forEach(el => el.style.display = '');
        document.querySelectorAll('.non-admin-only').forEach(el => el.style.display = 'none');
        // 系统配置分组默认折叠（条目多，避免撑满侧边栏）
        const adminNav = document.getElementById('nav-section-admin');
        if (adminNav && adminNav.classList.contains('open')) adminNav.classList.remove('open');
        document.querySelectorAll('.navbar-nav .nav-item').forEach(item => item.style.display = '');
        document.querySelectorAll('button[onclick*="export"]').forEach(btn => btn.style.display = '');
        const lofDash = document.getElementById('lof-dashboard-section');
        const lofShort = document.getElementById('dash-lof-shortcut');
        if (lofDash) { lofDash.style.display = ''; loadLofStats(); loadLofOpportunities(); }
        if (lofShort) lofShort.style.display = '';
        return;
    }

    // 普通用户：隐藏管理员专属项，显示非管理员专属项
    document.querySelectorAll('.admin-only').forEach(el => el.style.display = 'none');
    document.querySelectorAll('.non-admin-only').forEach(el => el.style.display = '');
    document.querySelectorAll('.navbar-nav .nav-item').forEach(item => {
        if (!item.classList.contains('admin-only')) item.style.display = '';
    });

    // 为每个需要权限的导航项加锁图标
    document.querySelectorAll('.navbar-nav .nav-item').forEach(item => {
        if (item.classList.contains('admin-only')) return;
        const link = item.querySelector('a');
        if (!link) return;
        const onclick = link.getAttribute('onclick') || '';
        const m = onclick.match(/showTab\('([^']+)'/);
        if (!m) return;
        const req = TAB_PERM_REQUIRED[m[1]];
        if (req && !permissions.includes(req.perm)) {
            item.classList.add('nav-item-locked');
            const icon = document.createElement('i');
            icon.className = 'bi bi-lock-fill nav-lock-icon';
            icon.style.cssText = 'font-size:10px;color:#faad14;margin-left:auto;flex-shrink:0;';
            link.appendChild(icon);
        }
    });

    // 导出按钮权限由各导出函数入口判断，此处无需处理

    // LOF 仪表盘区块：有权限才加载和显示
    const hasLof = permissions.includes('lof_arbitrage');
    const lofDash = document.getElementById('lof-dashboard-section');
    const lofShort = document.getElementById('dash-lof-shortcut');
    if (lofDash) lofDash.style.display = hasLof ? '' : 'none';
    if (lofShort) lofShort.style.display = hasLof ? '' : 'none';
    if (hasLof) { loadLofStats(); loadLofOpportunities(); }
}

// 导出权限检查：无权限时弹升级提示，返回 false
function requireExportPerm() {
    if (currentUser && currentUser.permissions && currentUser.permissions.includes('export_excel')) return true;
    showUpgradeModal('专业版', 'export_excel', null, null);
    return false;
}

// 导出错误处理：权限不足时弹升级提示，其他错误显示 alert
function handleExportError(errorMsg) {
    if (errorMsg && errorMsg.includes('需要权限')) {
        showUpgradeModal('专业版', 'export_excel', null, null);
    } else {
        alert('导出失败: ' + errorMsg);
    }
}

// 功能解锁弹窗（支持点数解锁 + 订阅升级）
let _unlockTargetTab = null;
let _unlockTargetEl = null;

function showUpgradeModal(requiredPlan, permCode, tabName, el) {
    _unlockTargetTab = tabName || null;
    _unlockTargetEl = el || null;

    // 从权限表找出对应的点数费用
    const UNLOCK_COSTS = {
        'stock_analysis_multi': 5, 'month_filter': 5, 'industry_statistics': 5,
        'source_compare': 5, 'month_enhanced': 8, 'industry_enhanced': 8, 'lof_arbitrage': 12,
    };
    const cost = permCode ? (UNLOCK_COSTS[permCode] || null) : null;
    const credits = currentUser && currentUser.credits ? currentUser.credits.total : 0;
    const canUnlock = cost !== null && credits >= cost;
    const alreadyUnlocked = cost === null; // export_excel 等不支持点数解锁

    let modal = document.getElementById('upgrade-hint-modal');
    if (modal) modal.remove();

    modal = document.createElement('div');
    modal.id = 'upgrade-hint-modal';
    modal.style.cssText = 'position:fixed;inset:0;z-index:9999;display:flex;align-items:center;justify-content:center;background:rgba(0,0,0,0.45);';

    const unlockSection = (cost !== null) ? `
        <div style="border:1px solid #e5e7eb;border-radius:8px;padding:14px 16px;margin-bottom:10px;text-align:left;">
            <div style="font-size:13px;font-weight:600;color:#111827;margin-bottom:6px;">
                <i class="bi bi-coin" style="color:#f59e0b;"></i> 点数解锁今日
            </div>
            <div style="font-size:13px;color:#6b7280;margin-bottom:10px;">
                消耗 <b style="color:#1677ff;">${cost} 点</b>，今日无限使用该功能<br>
                当前余额：<b id="unlock-modal-balance" style="color:${credits >= cost ? '#16a34a' : '#dc2626'}">${credits} 点</b><br>
                <span style="color:#f59e0b;font-size:12px;"><i class="bi bi-clock"></i> 今日有效，次日 00:00 自动重置</span>
            </div>
            ${canUnlock
                ? `<button id="unlock-modal-btn" onclick="doUnlockFromModal('${permCode}')"
                    style="width:100%;background:#1677ff;color:#fff;border:none;border-radius:7px;padding:9px 0;font-size:14px;font-weight:600;cursor:pointer;">
                    确认解锁（-${cost}点）
                   </button>`
                : `<a href="/pricing#credits" style="display:block;text-align:center;background:#f0f2f5;color:#374151;border-radius:7px;padding:9px 0;font-size:14px;text-decoration:none;">
                    点数不足，去充值 →
                   </a>`
            }
        </div>` : '';

    modal.innerHTML = `
        <div style="background:#fff;border-radius:12px;padding:28px 24px;max-width:340px;width:92%;box-shadow:0 8px 32px rgba(0,0,0,0.18);">
            <div style="font-size:16px;font-weight:700;color:#1a1a1a;margin-bottom:4px;">需要解锁此功能</div>
            <div style="font-size:13px;color:#6b7280;margin-bottom:16px;">该功能需要 <b>${requiredPlan}</b> 订阅${cost !== null ? '或点数解锁' : ''}</div>
            ${unlockSection}
            <a href="/pricing" style="display:block;text-align:center;background:#111827;color:#fff;border-radius:7px;padding:9px 0;font-size:14px;font-weight:600;text-decoration:none;margin-bottom:8px;">
                订阅${requiredPlan}，无限使用 →
            </a>
            <button onclick="document.getElementById('upgrade-hint-modal').remove()"
                style="width:100%;background:none;border:1px solid #e5e7eb;border-radius:7px;padding:8px 0;font-size:13px;color:#9ca3af;cursor:pointer;">
                稍后再说
            </button>
        </div>`;
    modal.addEventListener('click', e => { if (e.target === modal) modal.remove(); });
    document.body.appendChild(modal);
}

async function doUnlockFromModal(permCode) {
    const btn = document.getElementById('unlock-modal-btn');
    if (btn) { btn.disabled = true; btn.textContent = '解锁中…'; }
    try {
        const res = await fetch('/api/credits/unlock', {
            method: 'POST', credentials: 'include',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ permission_code: permCode }),
        });
        const d = await res.json();
        if (d.success) {
            // 更新本地余额
            if (currentUser && currentUser.credits) currentUser.credits.total = d.balance_after;
            document.getElementById('upgrade-hint-modal').remove();
            updateCreditsDisplay(d.balance_after);
            // 无论是否有目标 tab，都立即写入权限，避免再次触发解锁弹窗
            if (currentUser && !currentUser.permissions.includes(permCode)) {
                currentUser.permissions.push(permCode);
            }
            // 跳转到目标 tab（tab类功能）；无 tab 时触发原始元素（如单选按钮）
            if (_unlockTargetTab) {
                showTab(_unlockTargetTab, _unlockTargetEl);
            } else if (_unlockTargetEl) {
                _unlockTargetEl.click();
            }
        } else {
            if (btn) { btn.disabled = false; btn.textContent = `确认解锁`; }
            alert(d.detail || d.message || '解锁失败');
        }
    } catch(e) {
        if (btn) { btn.disabled = false; btn.textContent = `确认解锁`; }
    }
}

function updateCreditsDisplay(total) {
    const el = document.getElementById('sidebar-credits-balance');
    if (el) el.textContent = `${total} 点`;
    const el2 = document.getElementById('popup-credits-balance');
    if (el2) el2.textContent = `${total} 点`;
}

// 登录
function switchAuthTab(tab) {
    const loginForm = document.getElementById('login-form');
    const registerForm = document.getElementById('register-form');
    const tabLogin = document.getElementById('tab-login');
    const tabRegister = document.getElementById('tab-register');
    const errorDiv = document.getElementById('login-error');
    const successDiv = document.getElementById('register-success');
    errorDiv.style.display = 'none';
    successDiv.style.display = 'none';
    if (tab === 'login') {
        loginForm.style.display = '';
        registerForm.style.display = 'none';
        tabLogin.classList.add('active');
        tabRegister.classList.remove('active');
    } else {
        loginForm.style.display = 'none';
        registerForm.style.display = '';
        tabLogin.classList.remove('active');
        tabRegister.classList.add('active');
    }
}

async function register() {
    const username = document.getElementById('reg-username').value.trim();
    const password = document.getElementById('reg-password').value;
    const password2 = document.getElementById('reg-password2').value;
    const errorDiv = document.getElementById('login-error');
    const successDiv = document.getElementById('register-success');
    errorDiv.style.display = 'none';
    successDiv.style.display = 'none';

    if (password !== password2) {
        errorDiv.textContent = '两次输入的密码不一致';
        errorDiv.style.display = 'block';
        return;
    }

    try {
        const response = await fetch('/api/auth/register', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username, password })
        });
        const result = await response.json();
        if (result.success) {
            successDiv.textContent = result.gift_pending
                ? '注册成功！您的赠送点数需人工审核后发放，审核通过后自动到账，通常在24小时内完成。'
                : '注册成功！请登录。';
            successDiv.style.display = 'block';
            document.getElementById('register-form').reset();
            setTimeout(() => switchAuthTab('login'), result.gift_pending ? 4000 : 1500);
        } else {
            errorDiv.textContent = result.message || '注册失败';
            errorDiv.style.display = 'block';
        }
    } catch (error) {
        errorDiv.textContent = '注册失败: ' + error.message;
        errorDiv.style.display = 'block';
    }
}

async function login() {
    const username = document.getElementById('login-username').value.trim();
    const password = document.getElementById('login-password').value;
    const errorDiv = document.getElementById('login-error');
    
    if (!username || !password) {
        errorDiv.textContent = '请输入用户名和密码';
        errorDiv.style.display = 'block';
        return;
    }
    
    try {
        const response = await fetch('/api/auth/login', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            credentials: 'include',
            body: JSON.stringify({ username, password })
        });
        
        const result = await response.json();
        
        if (result.success) {
            currentUser = result.user;
            // 确保权限信息存在
            if (!currentUser.permissions) {
                currentUser.permissions = [];
            }
            // 先更新UI，再加载内容
            updateUIByRole();
            updateUIByPermissions();
            showMainContent();
            updateFooterPlan(currentUser);
            updateTrialBanner(currentUser);
            errorDiv.style.display = 'none';
        } else {
            let errorMessage = result.message || '登录失败';
            // 如果是账号过期，显示更友好的提示
            if (errorMessage.includes('账号已过期') || errorMessage.includes('过期')) {
                errorMessage = '账号已过期，请联系管理员重新授权';
            }
            errorDiv.innerHTML = errorMessage.replace(/\n/g, '<br>');
            errorDiv.style.display = 'block';
        }
    } catch (error) {
        errorDiv.textContent = '登录失败: ' + error.message;
        errorDiv.style.display = 'block';
    }
}

// ========== 底部用户面板 ==========

function toggleUserPanel() {
    const popup   = document.getElementById('user-panel-popup');
    const footer  = document.getElementById('sidebar-footer');
    const chevron = document.getElementById('footer-chevron');
    if (!popup) return;
    const isOpen = popup.classList.contains('open');
    popup.classList.toggle('open', !isOpen);
    if (footer)  footer.classList.toggle('active', !isOpen);
    if (chevron) chevron.style.transform = isOpen ? '' : 'rotate(180deg)';
}

document.addEventListener('click', function(e) {
    const footer = document.getElementById('sidebar-footer');
    const popup  = document.getElementById('user-panel-popup');
    if (popup && popup.classList.contains('open') && footer && !footer.contains(e.target)) {
        popup.classList.remove('open');
        if (footer) footer.classList.remove('active');
        const chevron = document.getElementById('footer-chevron');
        if (chevron) chevron.style.transform = '';
    }
    const modal = document.getElementById('my-orders-modal');
    if (modal && e.target === modal) modal.style.display = 'none';
});

function updateFooterPlan(user) {
    if (!user) return;
    const planCode   = user.plan_code || 'free';
    const daysLeft   = user.days_left;
    const planNames  = { free: '免费版', basic: '基础版', pro: '专业版' };
    const planColors = { free: '#8c8c8c', basic: '#1677ff', pro: '#fa8c16' };
    const planName   = planNames[planCode] || '免费版';
    const color      = planColors[planCode] || '#8c8c8c';

    const footerInfo = document.getElementById('footer-plan-info');
    if (footerInfo) {
        footerInfo.innerHTML =
            `<span style="display:inline-block;padding:1px 5px;border-radius:3px;background:${color}22;color:${color};font-size:10px;font-weight:600;">${planName}</span>`
            + (daysLeft != null ? `<span style="margin-left:5px;">· 剩余 ${daysLeft} 天</span>` : '');
    }

    const popupBadge = document.getElementById('popup-plan-badge');
    if (popupBadge) { popupBadge.textContent = planName; popupBadge.style.cssText = `background:${color};color:#fff;`; }

    const popupExpiry = document.getElementById('popup-expiry');
    if (popupExpiry) {
        if (user.valid_until) {
            const vu = user.valid_until;
            popupExpiry.textContent = `到期：${vu.slice(0,4)}-${vu.slice(4,6)}-${vu.slice(6,8)}（剩余 ${daysLeft} 天）`;
        } else {
            popupExpiry.textContent = planCode === 'free' ? '永久有效' : '';
        }
    }
    const upgradeBtn = document.getElementById('popup-upgrade-btn');
    if (upgradeBtn) upgradeBtn.style.display = planCode === 'pro' ? 'none' : '';

    // 点数余额显示
    const creditsTotal = user.credits ? user.credits.total : 0;
    updateCreditsDisplay(creditsTotal);

    // 试用中时，在套餐标签后加提示
    if (user.trial_plan && user.trial_days_left > 0 && footerInfo) {
        const trialNames = { basic: '基础版', pro: '专业版' };
        const trialLabel = `<span style="margin-left:5px;color:#fa8c16;font-size:10px;">试用中 ${user.trial_days_left}天</span>`;
        footerInfo.innerHTML += trialLabel;
    }
}

function updateTrialBanner(user) {
    const bar = document.getElementById('user-trial-topbar');
    if (!bar) return;
    if (user && user.trial_plan && user.trial_days_left > 0) {
        const planNames = { basic: '基础版', pro: '专业版' };
        document.getElementById('user-trial-plan-name').textContent = planNames[user.trial_plan] || user.trial_plan;
        document.getElementById('user-trial-days-left').textContent = user.trial_days_left;
        bar.style.display = '';
        // 主内容区下移，避免被遮挡
        const main = document.getElementById('main-content') || document.querySelector('.main-content');
        if (main && bar.style.display !== 'none') {
            const cur = parseInt(getComputedStyle(main).paddingTop) || 0;
            if (cur < 38) main.style.paddingTop = (cur + 38) + 'px';
        }
    } else {
        bar.style.display = 'none';
    }
}

async function showMyOrdersModal() {
    const popup = document.getElementById('user-panel-popup');
    if (popup) { popup.classList.remove('open'); }
    const footer = document.getElementById('sidebar-footer');
    if (footer) footer.classList.remove('active');
    const chevron = document.getElementById('footer-chevron');
    if (chevron) chevron.style.transform = '';

    const modal = document.getElementById('my-orders-modal');
    if (!modal) return;
    modal.style.display = 'flex';

    try {
        const res = await fetch('/api/my/subscription', { credentials: 'include' });
        const d = await res.json();
        if (d.success) {
            const s = d.data;
            const planNames  = { free: '免费版', basic: '基础版', pro: '专业版' };
            const planColors = { free: '#8c8c8c', basic: '#1677ff', pro: '#fa8c16' };
            const badge = document.getElementById('modal-plan-badge');
            if (badge) { badge.textContent = planNames[s.plan_code] || s.plan_name; badge.style.cssText = `background:${planColors[s.plan_code]||'#8c8c8c'};color:#fff;`; }
            const expiry = document.getElementById('modal-expiry-info');
            if (expiry) {
                if (s.valid_until) {
                    const vu = s.valid_until;
                    expiry.textContent = `到期：${vu.slice(0,4)}-${vu.slice(4,6)}-${vu.slice(6,8)}（剩余 ${s.days_left} 天）`;
                } else { expiry.textContent = s.plan_code === 'free' ? '免费版，永久有效' : ''; }
            }
            const upgradeBtn = document.getElementById('modal-upgrade-btn');
            if (upgradeBtn) upgradeBtn.style.display = s.plan_code === 'pro' ? 'none' : '';
        }
    } catch(e) {}

    const tbody = document.getElementById('modal-orders-tbody');
    if (!tbody) return;
    try {
        const res = await fetch('/api/my/orders', { credentials: 'include' });
        const d = await res.json();
        if (!d.success || !d.data.length) {
            tbody.innerHTML = '<tr><td colspan="6" class="text-center text-muted py-4">暂无订单记录</td></tr>';
            return;
        }
        const statusColor = { '已支付': '#52c41a', '待支付': '#faad14', '已过期': '#8c8c8c', '支付失败': '#ff4d4f' };
        tbody.innerHTML = d.data.map(o => {
            const paid = o.paid_at ? `${o.paid_at.slice(0,4)}-${o.paid_at.slice(4,6)}-${o.paid_at.slice(6,8)} ${o.paid_at.slice(8,10)}:${o.paid_at.slice(10,12)}` : '-';
            return `<tr>
                <td style="font-size:11px;color:#999;">${o.id.slice(-8)}</td>
                <td>${o.plan_name}</td><td>${o.billing_name}</td>
                <td>¥${o.amount_yuan.toFixed(2)}</td>
                <td><span style="color:${statusColor[o.status_name]||'#8c8c8c'};font-size:12px;">● ${o.status_name}</span></td>
                <td style="font-size:12px;color:#666;">${paid}</td>
            </tr>`;
        }).join('');
    } catch(e) {
        tbody.innerHTML = '<tr><td colspan="6" class="text-center text-danger py-3">加载失败</td></tr>';
    }
}

// 登出
async function logout() {
    try {
        await fetch('/api/auth/logout', {
            method: 'POST',
            credentials: 'include'
        });
    } catch (error) {
        console.error('登出失败:', error);
    } finally {
        window.location.reload();
    }
}

// 加载用户列表
async function loadUsers() {
    try {
        const response = await fetch('/api/users', {
            credentials: 'include'
        });
        const result = await response.json();
        
        if (result.success) {
            displayUsersList(result.data);
        }
    } catch (error) {
        console.error('加载用户列表失败:', error);
    }
}

// 显示用户列表
function displayUsersList(users) {
    const listDiv = document.getElementById('users-list');
    let html = '<div class="table-responsive"><table class="table table-striped table-hover">';
    html += '<thead><tr><th>ID</th><th>用户名</th><th>邮箱</th><th>角色</th><th>状态</th><th>有效期</th><th>创建时间</th><th>点数余额</th><th>点数调整</th><th>操作</th></tr></thead><tbody>';

    users.forEach(user => {
        const roleBadge = user.role === 'admin' ? '<span class="badge bg-danger">管理员</span>' : '<span class="badge bg-secondary">普通用户</span>';
        const statusBadge = user.is_active ? '<span class="badge bg-success">启用</span>' : '<span class="badge bg-danger">禁用</span>';
        const validUntil = user.valid_until ? new Date(
            user.valid_until.substring(0,4) + '-' +
            user.valid_until.substring(4,6) + '-' +
            user.valid_until.substring(6,8) + 'T' +
            user.valid_until.substring(8,10) + ':' +
            user.valid_until.substring(10,12) + ':' +
            user.valid_until.substring(12,14)
        ).toLocaleString('zh-CN') : '永久';
        const createdAt = new Date(
            user.created_at.substring(0,4) + '-' +
            user.created_at.substring(4,6) + '-' +
            user.created_at.substring(6,8) + 'T' +
            user.created_at.substring(8,10) + ':' +
            user.created_at.substring(10,12) + ':' +
            user.created_at.substring(12,14)
        ).toLocaleString('zh-CN');
        const inputId = `credits-input-${user.id}`;
        const msgId   = `credits-msg-${user.id}`;

        const creditsHtml = user.role === 'admin' ? '<span class="text-muted small">-</span>' :
            `<span style="color:#1677ff;font-weight:600;">${user.credits_total ?? 0}</span>
             <span class="text-muted small">（付费 ${user.credits_balance ?? 0} · 赠送 ${user.credits_gift ?? 0}）</span>`;
        html += `<tr>
            <td>${user.id}</td>
            <td>${user.username}</td>
            <td><span class="text-muted small">${user.email || '<i>未填写</i>'}</span></td>
            <td>${roleBadge}</td>
            <td>${statusBadge}</td>
            <td>${validUntil}</td>
            <td>${createdAt}</td>
            <td>${creditsHtml}</td>
            <td>
                <div class="d-flex align-items-center gap-1" style="min-width:160px;">
                    <input type="number" id="${inputId}" class="form-control form-control-sm" placeholder="±点数" style="width:80px;">
                    <button class="btn btn-sm btn-outline-primary" onclick="adminAdjustCredits(${user.id},'${inputId}','${msgId}')">调整</button>
                </div>
                <div id="${msgId}" style="font-size:11px;min-height:16px;"></div>
            </td>
            <td>
                ${user.role !== 'admin' ? `<button class="btn btn-sm btn-info" onclick="showPermissionModal(${user.id})">权限</button>` : '<span class="badge bg-success">全部权限</span>'}
                <button class="btn btn-sm btn-warning" onclick="showEditUserModal(${user.id})">编辑</button>
                ${user.id !== currentUser.id ? `<button class="btn btn-sm btn-danger" onclick="deleteUser(${user.id})">删除</button>` : ''}
            </td>
        </tr>`;
    });

    html += '</tbody></table></div>';
    listDiv.innerHTML = html;
}

// 显示添加用户模态框
function showAddUserModal() {
    document.getElementById('add-username').value = '';
    document.getElementById('add-password').value = '';
    document.getElementById('add-role').value = 'user';
    document.getElementById('add-valid-until').value = '';
    const modal = new bootstrap.Modal(document.getElementById('addUserModal'));
    modal.show();
}

// 添加用户
async function addUser() {
    const username = document.getElementById('add-username').value.trim();
    const password = document.getElementById('add-password').value;
    const role = document.getElementById('add-role').value;
    const validUntil = document.getElementById('add-valid-until').value;
    
    if (!username || !password) {
        alert('用户名和密码不能为空');
        return;
    }
    
    // 转换日期格式
    let validUntilFormatted = null;
    if (validUntil) {
        const date = new Date(validUntil);
        validUntilFormatted = date.getFullYear().toString() + 
                            (date.getMonth() + 1).toString().padStart(2, '0') +
                            date.getDate().toString().padStart(2, '0') +
                            date.getHours().toString().padStart(2, '0') +
                            date.getMinutes().toString().padStart(2, '0') +
                            date.getSeconds().toString().padStart(2, '0');
    }
    
    try {
        const response = await fetch('/api/users', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            credentials: 'include',
            body: JSON.stringify({
                username,
                password,
                role,
                valid_until: validUntilFormatted
            })
        });
        
        const result = await response.json();
        
        if (result.success) {
            alert('用户添加成功');
            bootstrap.Modal.getInstance(document.getElementById('addUserModal')).hide();
            loadUsers();
        } else {
            alert('添加用户失败: ' + result.message);
        }
    } catch (error) {
        alert('添加用户失败: ' + error.message);
    }
}

// 编辑用户（使用模态框）
let currentEditUserId = null;

async function showEditUserModal(userId) {
    currentEditUserId = userId;
    
    try {
        // 获取用户信息
        const response = await fetch(`/api/users/${userId}`, {
            credentials: 'include'
        });
        const result = await response.json();
        
        if (result.success) {
            const user = result.data;
            
            // 填充表单
            document.getElementById('edit-username').value = user.username || '';
            document.getElementById('edit-password').value = '';
            document.getElementById('edit-role').value = user.role || 'user';
            document.getElementById('edit-is-active').checked = user.is_active !== false;
            
            // 处理有效期
            if (user.valid_until) {
                const dateStr = user.valid_until;
                // 格式：YYYYMMDDHHMMSS -> YYYY-MM-DDTHH:MM
                const year = dateStr.substring(0, 4);
                const month = dateStr.substring(4, 6);
                const day = dateStr.substring(6, 8);
                const hour = dateStr.substring(8, 10);
                const minute = dateStr.substring(10, 12);
                document.getElementById('edit-valid-until').value = `${year}-${month}-${day}T${hour}:${minute}`;
            } else {
                document.getElementById('edit-valid-until').value = '';
            }
            
            // 显示模态框
            const modal = new bootstrap.Modal(document.getElementById('editUserModal'));
            modal.show();
        } else {
            alert('获取用户信息失败: ' + result.message);
        }
    } catch (error) {
        alert('获取用户信息失败: ' + error.message);
    }
}

// 更新用户
async function updateUser() {
    if (!currentEditUserId) {
        return;
    }
    
    const username = document.getElementById('edit-username').value.trim();
    const password = document.getElementById('edit-password').value;
    const role = document.getElementById('edit-role').value;
    const isActive = document.getElementById('edit-is-active').checked;
    const validUntil = document.getElementById('edit-valid-until').value;
    
    if (!username) {
        alert('用户名不能为空');
        return;
    }
    
    // 转换日期格式
    let validUntilFormatted = null;
    if (validUntil) {
        const date = new Date(validUntil);
        validUntilFormatted = date.getFullYear().toString() + 
                            (date.getMonth() + 1).toString().padStart(2, '0') +
                            date.getDate().toString().padStart(2, '0') +
                            date.getHours().toString().padStart(2, '0') +
                            date.getMinutes().toString().padStart(2, '0') +
                            date.getSeconds().toString().padStart(2, '0');
    }
    
    try {
        const updateData = {
            username: username,
            role: role,
            is_active: isActive
        };
        
        // 只有输入了密码才更新
        if (password) {
            updateData.password = password;
        }
        
        if (validUntilFormatted) {
            updateData.valid_until = validUntilFormatted;
        } else {
            updateData.valid_until = null;
        }
        
        const response = await fetch(`/api/users/${currentEditUserId}`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json'
            },
            credentials: 'include',
            body: JSON.stringify(updateData)
        });
        
        const result = await response.json();
        
        if (result.success) {
            alert('用户信息已更新');
            bootstrap.Modal.getInstance(document.getElementById('editUserModal')).hide();
            loadUsers();
        } else {
            alert('更新用户失败: ' + result.message);
        }
    } catch (error) {
        alert('更新用户失败: ' + error.message);
    }
}

// 删除用户
async function deleteUser(userId) {
    if (!confirm('确定要删除该用户吗？此操作不可恢复！')) {
        return;
    }
    
    try {
        const response = await fetch(`/api/users/${userId}`, {
            method: 'DELETE',
            credentials: 'include'
        });
        
        const result = await response.json();
        
        if (result.success) {
            alert('用户已删除');
            loadUsers();
        } else {
            alert('删除用户失败: ' + result.message);
        }
    } catch (error) {
        alert('删除用户失败: ' + error.message);
    }
}

// 修改密码
async function changePassword() {
    const oldPassword = document.getElementById('old-password').value;
    const newPassword = document.getElementById('new-password').value;
    
    if (!oldPassword || !newPassword) {
        alert('请输入旧密码和新密码');
        return;
    }
    
    try {
        const response = await fetch('/api/auth/change-password', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            credentials: 'include',
            body: JSON.stringify({
                old_password: oldPassword,
                new_password: newPassword
            })
        });
        
        const result = await response.json();
        
        if (result.success) {
            alert('密码修改成功');
            document.getElementById('old-password').value = '';
            document.getElementById('new-password').value = '';
        } else {
            alert('修改密码失败: ' + result.message);
        }
    } catch (error) {
        alert('修改密码失败: ' + error.message);
    }
}

// 加载系统配置
async function loadSystemConfig() {
    try {
        const response = await fetch('/api/system/config', {
            credentials: 'include'
        });
        const result = await response.json();

        if (result.success) {
            document.getElementById('session-duration').value = result.data.session_duration_hours;
            const regCheckbox = document.getElementById('registration-enabled');
            if (regCheckbox) {
                regCheckbox.checked = result.data.registration_enabled;
                document.getElementById('registration-enabled-label').textContent = result.data.registration_enabled ? '已开放' : '已关闭';
            }
            const giftCheckbox = document.getElementById('gift-credits-enabled');
            if (giftCheckbox) {
                giftCheckbox.checked = result.data.gift_credits_enabled !== false;
                document.getElementById('gift-credits-label').textContent = giftCheckbox.checked ? '已开启' : '已关闭';
                document.getElementById('gift-ip-review').value = result.data.gift_ip_review ?? 2;
            }
            if (typeof result.data.reg_email_verify !== 'undefined') {
                const el = document.getElementById('reg-email-verify-toggle');
                if (el) el.checked = !!result.data.reg_email_verify;
            }
            // SMTP
            const smtp = result.data.smtp || {};
            document.getElementById('smtp-host').value         = smtp.host || '';
            document.getElementById('smtp-port').value         = smtp.port || 465;
            document.getElementById('smtp-ssl').value          = smtp.use_ssl === false ? 'false' : 'true';
            document.getElementById('smtp-user').value         = smtp.user || '';
            document.getElementById('smtp-password').value     = smtp.password || '';
            document.getElementById('smtp-from-addr').value    = smtp.from_addr || '';
        }
    } catch (error) {
        console.error('加载系统配置失败:', error);
    }
}

// 保存系统配置
async function saveSystemConfig() {
    const sessionDuration = parseInt(document.getElementById('session-duration').value);
    
    if (!sessionDuration || sessionDuration < 1 || sessionDuration > 8760) {
        alert('会话时长必须在1-8760小时之间');
        return;
    }
    
    try {
        const response = await fetch('/api/system/config', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            credentials: 'include',
            body: JSON.stringify({
                session_duration_hours: sessionDuration,
                registration_enabled:  document.getElementById('registration-enabled')?.checked ?? true,
                gift_credits_enabled:  document.getElementById('gift-credits-enabled')?.checked ?? true,
                gift_ip_review:        parseInt(document.getElementById('gift-ip-review')?.value || '2'),
                reg_email_verify:      document.getElementById('reg-email-verify-toggle')?.checked || false,
            })
        });
        
        const result = await response.json();
        
        if (result.success) {
            alert('系统配置已保存');
        } else {
            alert('保存系统配置失败: ' + result.message);
        }
    } catch (error) {
        alert('保存系统配置失败: ' + error.message);
    }
}

// 权限管理
let currentPermissionUserId = null;

// 显示权限管理模态框
async function showPermissionModal(userId) {
    currentPermissionUserId = userId;
    
    try {
        // 获取所有权限列表
        const permissionsResponse = await fetch('/api/permissions', {
            credentials: 'include'
        });
        const permissionsResult = await permissionsResponse.json();
        
        // 获取用户当前权限
        const userPermResponse = await fetch(`/api/users/${userId}/permissions`, {
            credentials: 'include'
        });
        const userPermResult = await userPermResponse.json();
        
        if (permissionsResult.success && userPermResult.success) {
            const allPermissions = permissionsResult.data;
            const userPermissions = userPermResult.data || [];
            
            // 获取用户名
            const usersResponse = await fetch('/api/users', {
                credentials: 'include'
            });
            const usersResult = await usersResponse.json();
            const user = usersResult.data.find(u => u.id === userId);
            const username = user ? user.username : '未知用户';
            
            document.getElementById('permission-username').textContent = username;
            
            // 显示权限列表
            let html = '<div class="list-group">';
            allPermissions.forEach(perm => {
                const isChecked = userPermissions.includes(perm.code);
                html += `
                    <div class="list-group-item">
                        <div class="form-check">
                            <input class="form-check-input" type="checkbox" 
                                   id="perm-${perm.code}" 
                                   value="${perm.code}" 
                                   ${isChecked ? 'checked' : ''}>
                            <label class="form-check-label" for="perm-${perm.code}">
                                <strong>${perm.name}</strong>
                                <br><small class="text-muted">${perm.description}</small>
                            </label>
                        </div>
                    </div>
                `;
            });
            html += '</div>';
            
            document.getElementById('permissions-list').innerHTML = html;
            
            const modal = new bootstrap.Modal(document.getElementById('permissionModal'));
            modal.show();
        }
    } catch (error) {
        alert('加载权限失败: ' + error.message);
    }
}

// 保存权限
async function savePermissions() {
    if (!currentPermissionUserId) {
        return;
    }
    
    const checkboxes = document.querySelectorAll('#permissions-list input[type="checkbox"]');
    const selectedPermissions = Array.from(checkboxes)
        .filter(cb => cb.checked)
        .map(cb => cb.value);
    
    try {
        const response = await fetch(`/api/users/${currentPermissionUserId}/permissions`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json'
            },
            credentials: 'include',
            body: JSON.stringify({
                permissions: selectedPermissions
            })
        });
        
        const result = await response.json();
        
        if (result.success) {
            alert('权限已保存');
            bootstrap.Modal.getInstance(document.getElementById('permissionModal')).hide();
            loadUsers(); // 重新加载用户列表
        } else {
            alert('保存权限失败: ' + result.message);
        }
    } catch (error) {
        alert('保存权限失败: ' + error.message);
    }
}

// 为表格列头添加点击排序
function makeSortable(table) {
    if (!table) return;
    const headers = table.querySelectorAll('thead th');
    headers.forEach((th, colIdx) => {
        th.classList.add('sortable-th');
        let asc = true;
        th.addEventListener('click', function () {
            const tbody = table.querySelector('tbody');
            if (!tbody) return;
            const rows = Array.from(tbody.querySelectorAll('tr'));
            rows.sort((a, b) => {
                const aText = (a.cells[colIdx]?.textContent || '').trim().replace(/[%,]/g, '');
                const bText = (b.cells[colIdx]?.textContent || '').trim().replace(/[%,]/g, '');
                const aNum = parseFloat(aText);
                const bNum = parseFloat(bText);
                if (!isNaN(aNum) && !isNaN(bNum)) return asc ? aNum - bNum : bNum - aNum;
                return asc ? aText.localeCompare(bText, 'zh') : bText.localeCompare(aText, 'zh');
            });
            rows.forEach(r => tbody.appendChild(r));
            headers.forEach(h => h.classList.remove('sort-asc', 'sort-desc'));
            th.classList.add(asc ? 'sort-asc' : 'sort-desc');
            asc = !asc;
        });
    });
}

// ===== LOF仪表盘 =====

async function loadLofStats() {
    try {
        const res = await fetch('/lof1/api/arbitrage/statistics');
        if (!res.ok) return;
        const data = await res.json();
        if (!data.success) return;
        const s = data.statistics;
        const profitColor = s.total_net_profit >= 0 ? 'text-success' : 'text-danger';
        const profitSign = s.total_net_profit >= 0 ? '+' : '';
        document.getElementById('lof-stats-cards').innerHTML = `
            <div class="col-6 col-md-3">
                <div class="border rounded p-3 text-center h-100">
                    <div class="fs-4 fw-bold">${s.total_count ?? 0}</div>
                    <div class="small text-muted">累计完成</div>
                </div>
            </div>
            <div class="col-6 col-md-3">
                <div class="border rounded p-3 text-center h-100">
                    <div class="fs-4 fw-bold ${profitColor}">${profitSign}¥${(s.total_net_profit ?? 0).toFixed(2)}</div>
                    <div class="small text-muted">累计净收益</div>
                </div>
            </div>
            <div class="col-6 col-md-3">
                <div class="border rounded p-3 text-center h-100">
                    <div class="fs-4 fw-bold">${s.win_rate ?? 0}%</div>
                    <div class="small text-muted">胜率</div>
                </div>
            </div>
            <div class="col-6 col-md-3">
                <div class="border rounded p-3 text-center h-100">
                    <div class="fs-4 fw-bold text-primary">${s.in_progress_count ?? 0}</div>
                    <div class="small text-muted">进行中</div>
                </div>
            </div>`;
    } catch (e) {
        document.getElementById('lof-stats-cards').innerHTML = '<div class="col text-muted small">暂无数据</div>';
    }
}

async function loadLofOpportunities() {
    const minPct = document.getElementById('lof-opp-min-pct')?.value ?? 10;
    const container = document.getElementById('lof-opportunities-table');
    const totalBadge = document.getElementById('lof-opp-total');
    container.innerHTML = '<div class="p-3 text-center text-muted small">加载中...</div>';
    try {
        const params = new URLSearchParams({ min_pct: minPct, limit: 20 });
        const res = await fetch(`/lof1/api/funds/opportunities?${params}`);
        if (!res.ok) { container.innerHTML = '<div class="p-3 text-muted small">接口暂不可用</div>'; return; }
        const data = await res.json();
        if (!data.success) { container.innerHTML = `<div class="p-3 text-muted small">${data.message || '加载失败'}</div>`; return; }

        totalBadge.textContent = data.total > 0 ? data.total : '';
        if (!data.data || data.data.length === 0) {
            container.innerHTML = '<div class="p-3 text-center text-muted small">当前无满足条件的套利机会</div>';
            return;
        }

        let html = `<div class="table-responsive">
            <table class="table table-sm table-hover mb-0" style="font-size:13px;">
                <thead class="table-light">
                    <tr>
                        <th>基金</th>
                        <th>类型</th>
                        <th class="text-end">溢价率</th>
                        <th class="text-end">套利收益率</th>
                        <th class="text-end">万元净收益</th>
                        <th class="text-end">场内价</th>
                        <th class="text-end">净值</th>
                    </tr>
                </thead><tbody>`;
        for (const f of data.data) {
            const isPremium = f.arbitrage_type === '溢价套利';
            const badge = isPremium
                ? '<span class="badge bg-warning text-dark">溢价</span>'
                : '<span class="badge bg-info text-dark">折价</span>';
            const diffColor = isPremium ? 'text-warning fw-bold' : 'text-info fw-bold';
            const profitColor = f.profit_rate > 0 ? 'text-success' : 'text-muted';
            const sign = f.price_diff_pct > 0 ? '+' : '';
            html += `<tr>
                <td><span class="text-muted small">${f.fund_code}</span> ${f.fund_name || ''}</td>
                <td>${badge}</td>
                <td class="text-end ${diffColor}">${sign}${f.price_diff_pct.toFixed(2)}%</td>
                <td class="text-end ${profitColor}">${f.profit_rate.toFixed(2)}%</td>
                <td class="text-end fw-bold">¥${f.net_profit_10k.toFixed(0)}</td>
                <td class="text-end">${f.price.toFixed(3)}</td>
                <td class="text-end">${f.nav.toFixed(4)}</td>
            </tr>`;
        }
        html += `</tbody></table></div>`;
        if (data.total > data.data.length) {
            html += `<div class="px-3 py-2 text-muted small border-top">仅显示前 ${data.data.length} 条，共 ${data.total} 个机会</div>`;
        }
        container.innerHTML = html;
    } catch (e) {
        container.innerHTML = '<div class="p-3 text-muted small">加载失败: ' + e.message + '</div>';
    }
}

// ===== LOF基金套利 数据库管理 =====
async function lofBackupDatabase() {
    if (!confirm('确定要备份 LOF套利 数据库吗？这将下载套利记录、收藏和通知数据。')) return;
    try {
        const filename = await _fetchDownload('/lof1/api/admin/backup', '正在备份LOF数据库…');
        showToast('LOF备份成功：' + filename, 'success');
    } catch(e) {
        _hideProgress(false);
        alert('备份失败：' + e.message);
    }
}

async function lofRestoreDatabase(input) {
    const file = input.files[0];
    if (!file) return;
    if (!confirm('警告：还原将覆盖当前所有 LOF套利 数据！原数据会自动备份。\n\n确定继续？')) {
        input.value = ''; return;
    }
    const formData = new FormData();
    formData.append('backup_file', file);
    try {
        const result = await _xhrUpload('/lof1/api/admin/restore', formData, '正在上传LOF备份…', '正在还原LOF数据…');
        input.value = '';
        if (result.success) {
            _hideProgress(true);
            showToast('LOF还原成功！', 'success');
        } else {
            _hideProgress(false);
            alert('还原失败：' + (result.message || '未知错误'));
        }
    } catch(e) {
        input.value = '';
        _hideProgress(false);
        alert('还原失败：' + e.message);
    }
}




// ===== 行业增强分析 =====
let _enhancedData = [];

async function analyzeEnhancedIndustry() {
    const market      = document.getElementById('enhanced-market').value;
    const industryType= document.getElementById('enhanced-industry-type').value;
    const month       = parseInt(document.getElementById('enhanced-month').value);
    const startYear   = parseInt(document.getElementById('enhanced-start-year').value);
    const endYearEl   = document.getElementById('enhanced-end-year');
    const endYear     = endYearEl.value ? parseInt(endYearEl.value) : new Date().getFullYear();
    const resultDiv   = document.getElementById('enhanced-result');

    resultDiv.innerHTML = '<div class="text-center py-4"><div class="spinner-border text-primary"></div><p class="mt-2 text-muted">计算中，数据量大时可能需要数秒...</p></div>';

    try {
        const excludeRelisting = document.getElementById('enhanced-exclude-relisting')?.checked || false;
        const res = await fetch('/api/industry/enhanced-stats', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'include',
            body: JSON.stringify({ market: market || null, industry_type: industryType, month, start_year: startYear, end_year: endYear, exclude_relisting: excludeRelisting })
        });
        const result = await res.json();
        if (!result.success) { resultDiv.innerHTML = `<div class="alert alert-danger">${result.message || '查询失败'}</div>`; return; }
        if (!result.data || result.data.length === 0) { resultDiv.innerHTML = '<div class="alert alert-warning">暂无数据，请先更新行业分类。</div>'; return; }
        _enhancedData = result.data;
        renderEnhancedTable(_enhancedData);
    } catch (e) {
        resultDiv.innerHTML = `<div class="alert alert-danger">请求失败: ${e.message}</div>`;
    }
}

function sortEnhancedTable() {
    if (!_enhancedData.length) return;
    const key = document.getElementById('enhanced-sort-key').value;
    const dir = document.getElementById('enhanced-sort-dir').value;
    const sorted = [..._enhancedData].sort((a, b) => {
        const va = a[key] ?? -Infinity;
        const vb = b[key] ?? -Infinity;
        return dir === 'desc' ? vb - va : va - vb;
    });
    renderEnhancedTable(sorted);
}

function renderEnhancedTable(data) {
    const resultDiv = document.getElementById('enhanced-result');
    const fmt = (v, digits=2) => v == null ? '<span class="text-muted">-</span>' : v.toFixed(digits);
    const colorRet = v => v == null ? '' : v > 0 ? 'color:#16a34a;font-weight:600' : v < 0 ? 'color:#dc2626;font-weight:600' : '';
    const colorProb = v => v == null ? '' : v >= 60 ? 'color:#16a34a' : v <= 40 ? 'color:#dc2626' : '';
    const colorCons = v => v == null ? '' : v >= 80 ? 'color:#16a34a' : v <= 50 ? 'color:#dc2626' : 'color:#d97706';

    const rows = data.map((d, i) => `
        <tr>
            <td class="text-muted small">${i+1}</td>
            <td><strong>${d.industry_name}</strong></td>
            <td class="text-end small text-muted">${d.stock_count}</td>
            <td class="text-end small text-muted">${d.total_years}年</td>
            <td class="text-end" style="${colorRet(d.expected_return)}">${fmt(d.expected_return)}%</td>
            <td class="text-end" style="${colorProb(d.up_probability)}">${fmt(d.up_probability)}%</td>
            <td class="text-end" style="color:#16a34a">${fmt(d.avg_up_return)}%</td>
            <td class="text-end" style="color:#dc2626">${fmt(d.avg_down_return)}%</td>
            <td class="text-end" style="${colorProb(d.excess_market_prob)}">${fmt(d.excess_market_prob)}%</td>
            <td class="text-end" style="${colorProb(d.recent_up_prob)}">${d.recent_up_prob != null ? fmt(d.recent_up_prob)+'%' : '<span class="text-muted">-</span>'}</td>
            <td class="text-end" style="${colorCons(d.consistency)}">${d.consistency != null ? fmt(d.consistency) : '<span class="text-muted">-</span>'}</td>
        </tr>`).join('');

    resultDiv.innerHTML = `
        <div class="table-responsive">
            <table class="table table-sm table-hover align-middle" style="font-size:0.875rem">
                <thead class="table-light sticky-top">
                    <tr>
                        <th>#</th>
                        <th>行业名称</th>
                        <th class="text-end">股票数</th>
                        <th class="text-end">样本年数</th>
                        <th class="text-end text-primary">期望收益率</th>
                        <th class="text-end">上涨概率</th>
                        <th class="text-end">平均涨幅</th>
                        <th class="text-end">平均跌幅</th>
                        <th class="text-end">跑赢大盘概率</th>
                        <th class="text-end">近5年上涨率</th>
                        <th class="text-end">一致性</th>
                    </tr>
                </thead>
                <tbody>${rows}</tbody>
            </table>
        </div>
        <div class="text-muted small mt-1">共 ${data.length} 个行业</div>
        <div class="mt-3">
            <button class="btn btn-success" onclick="exportIndustryEnhanced()">
                <i class="bi bi-file-earmark-excel"></i> 导出Excel
            </button>
        </div>`;
}

// ===== 月榜单增强 =====
let _mEnhancedData = [];

async function analyzeEnhancedMonth() {
    const market    = document.getElementById('menhanced-market').value;
    const month     = parseInt(document.getElementById('menhanced-month').value);
    const startYear = parseInt(document.getElementById('menhanced-start-year').value);
    const endYearEl = document.getElementById('menhanced-end-year');
    const endYear   = endYearEl.value ? parseInt(endYearEl.value) : new Date().getFullYear();
    const minYears  = parseInt(document.getElementById('menhanced-min-years').value) || 3;
    const topN      = parseInt(document.getElementById('menhanced-top-n').value);
    const resultDiv = document.getElementById('menhanced-result');

    resultDiv.innerHTML = '<div class="text-center py-4"><div class="spinner-border text-primary"></div><p class="mt-2 text-muted">计算中，数据量大时可能需要数秒...</p></div>';

    try {
        const excludeRelisting = document.getElementById('menhanced-exclude-relisting')?.checked || false;
        const res = await fetch('/api/month/enhanced-stats', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'include',
            body: JSON.stringify({ market: market || null, month, start_year: startYear, end_year: endYear, min_years: minYears, top_n: topN, exclude_relisting: excludeRelisting })
        });
        const result = await res.json();
        if (!result.success) { resultDiv.innerHTML = `<div class="alert alert-danger">${result.message || '查询失败'}</div>`; return; }
        if (!result.data || result.data.length === 0) { resultDiv.innerHTML = '<div class="alert alert-warning">暂无数据，请先更新股票数据。</div>'; return; }
        _mEnhancedData = result.data;
        renderEnhancedMonthTable(_mEnhancedData);
    } catch (e) {
        resultDiv.innerHTML = `<div class="alert alert-danger">请求失败: ${e.message}</div>`;
    }
}

function sortEnhancedMonthTable() {
    if (!_mEnhancedData.length) return;
    const key = document.getElementById('menhanced-sort-key').value;
    const dir = document.getElementById('menhanced-sort-dir').value;
    const sorted = [..._mEnhancedData].sort((a, b) => {
        const va = a[key] ?? -Infinity;
        const vb = b[key] ?? -Infinity;
        return dir === 'desc' ? vb - va : va - vb;
    });
    renderEnhancedMonthTable(sorted);
}

function renderEnhancedMonthTable(data) {
    const resultDiv = document.getElementById('menhanced-result');
    const fmt = (v, digits=2) => v == null ? '<span class="text-muted">-</span>' : Number(v).toFixed(digits);
    const colorRet  = v => v == null ? '' : v > 0 ? 'color:#16a34a;font-weight:600' : v < 0 ? 'color:#dc2626;font-weight:600' : '';
    const colorProb = v => v == null ? '' : v >= 60 ? 'color:#16a34a' : v <= 40 ? 'color:#dc2626' : '';
    const colorCons = v => v == null ? '' : v >= 80 ? 'color:#16a34a' : v <= 50 ? 'color:#dc2626' : 'color:#d97706';

    const rows = data.map((d, i) => `
        <tr>
            <td class="text-muted small">${i+1}</td>
            <td><strong>${d.name}</strong><br><span class="text-muted small">${d.symbol || d.ts_code}</span></td>
            <td class="text-end small text-muted">${d.total_years}年</td>
            <td class="text-end" style="${colorRet(d.expected_return)}">${fmt(d.expected_return)}%</td>
            <td class="text-end" style="${colorProb(d.up_probability)}">${fmt(d.up_probability)}%</td>
            <td class="text-end" style="color:#16a34a">${fmt(d.avg_up_return)}%</td>
            <td class="text-end" style="color:#dc2626">${fmt(d.avg_down_return)}%</td>
            <td class="text-end" style="color:#16a34a;font-size:0.8em">${fmt(d.max_up)}%</td>
            <td class="text-end" style="color:#dc2626;font-size:0.8em">${fmt(d.max_down)}%</td>
            <td class="text-end" style="${colorProb(d.excess_market_prob)}">${fmt(d.excess_market_prob)}%</td>
            <td class="text-end" style="${colorProb(d.recent_up_prob)}">${d.recent_up_prob != null ? fmt(d.recent_up_prob)+'%' : '<span class="text-muted">-</span>'}</td>
            <td class="text-end" style="${colorCons(d.consistency)}">${d.consistency != null ? fmt(d.consistency) : '<span class="text-muted">-</span>'}</td>
        </tr>`).join('');

    resultDiv.innerHTML = `
        <div class="table-responsive">
            <table class="table table-sm table-hover align-middle" style="font-size:0.875rem">
                <thead class="table-light sticky-top">
                    <tr>
                        <th>#</th>
                        <th>股票</th>
                        <th class="text-end">样本年数</th>
                        <th class="text-end text-primary">期望收益率</th>
                        <th class="text-end">上涨概率</th>
                        <th class="text-end">平均涨幅</th>
                        <th class="text-end">平均跌幅</th>
                        <th class="text-end">最大涨幅</th>
                        <th class="text-end">最大跌幅</th>
                        <th class="text-end">跑赢大盘概率</th>
                        <th class="text-end">近5年上涨率</th>
                        <th class="text-end">一致性</th>
                    </tr>
                </thead>
                <tbody>${rows}</tbody>
            </table>
        </div>
        <div class="text-muted small mt-1">共 ${data.length} 条记录</div>
        <div class="mt-3">
            <button class="btn btn-success" onclick="exportMonthEnhanced()">
                <i class="bi bi-file-earmark-excel"></i> 导出Excel
            </button>
        </div>`;
}

async function exportIndustryEnhanced() {
    if (!requireExportPerm()) return;
    const endYearEl = document.getElementById('enhanced-end-year');
    try {
        const filename = await _postDownload('/api/export/industry-enhanced', {
            market: document.getElementById('enhanced-market').value || null,
            industry_type: document.getElementById('enhanced-industry-type').value,
            month: parseInt(document.getElementById('enhanced-month').value),
            start_year: parseInt(document.getElementById('enhanced-start-year').value),
            end_year: endYearEl.value ? parseInt(endYearEl.value) : new Date().getFullYear(),
            exclude_relisting: document.getElementById('enhanced-exclude-relisting')?.checked || false
        }, '正在导出行业增强分析…');
        showToast('导出成功：' + filename, 'success');
    } catch(e) {
        _hideProgress(false);
        handleExportError(e.message);
    }
}

async function exportMonthEnhanced() {
    if (!requireExportPerm()) return;
    const endYearEl = document.getElementById('menhanced-end-year');
    try {
        const filename = await _postDownload('/api/export/month-enhanced', {
            market: document.getElementById('menhanced-market').value || null,
            month: parseInt(document.getElementById('menhanced-month').value),
            start_year: parseInt(document.getElementById('menhanced-start-year').value),
            end_year: endYearEl.value ? parseInt(endYearEl.value) : new Date().getFullYear(),
            min_years: parseInt(document.getElementById('menhanced-min-years').value) || 3,
            top_n: parseInt(document.getElementById('menhanced-top-n').value),
            exclude_relisting: document.getElementById('menhanced-exclude-relisting')?.checked || false
        }, '正在导出月榜单增强…');
        showToast('导出成功：' + filename, 'success');
    } catch(e) {
        _hideProgress(false);
        handleExportError(e.message);
    }
}

// ===== 使用指南 =====
function helpScrollTo(sectionId, linkEl) {
    const target = document.getElementById(sectionId);
    if (target) target.scrollIntoView({ behavior: 'smooth', block: 'start' });
    document.querySelectorAll('.help-toc-link').forEach(a => a.classList.remove('active'));
    if (linkEl) linkEl.classList.add('active');
}

// ===== 数据管理 Tab 切换 =====
let _dataTabLoaded = { backup: false, kline: false, lof: false };

function switchDataTab(name, el) {
    ['backup', 'kline', 'lof'].forEach(t => {
        const pane = document.getElementById(`data-tab-${t}`);
        if (pane) pane.style.display = t === name ? '' : 'none';
    });
    document.querySelectorAll('#dataMgmtTabs .nav-link').forEach(a => a.classList.remove('active'));
    if (el) el.classList.add('active');
    if (!_dataTabLoaded[name]) {
        _dataTabLoaded[name] = true;
        if (name === 'backup') { loadBackupList(); loadBackupConfig(); }
    }
}

// ===== 套餐管理 Tab 切换 =====
let _planTabLoaded = { price: false, promo: false, trial: false, records: false };

function switchPlanTab(name, el) {
    ['price', 'promo', 'trial', 'records'].forEach(t => {
        const pane = document.getElementById(`plan-tab-${t}`);
        if (pane) pane.style.display = t === name ? '' : 'none';
    });
    document.querySelectorAll('#planMgmtTabs .nav-link').forEach(a => a.classList.remove('active'));
    if (el) el.classList.add('active');
    if (!_planTabLoaded[name]) {
        _planTabLoaded[name] = true;
        if (name === 'price')   loadPlanPrices();
        if (name === 'promo')   loadPromotions();
        if (name === 'trial')   loadTrials();
        if (name === 'records') loadTrialRecords();
    }
}

// ===== 套餐管理 =====
const PLAN_LABELS = { free: '免费版', basic: '基础版', pro: '专业版' };
const PLAN_PERMS = {
    free:  ['单股分析（单月统计）'],
    basic: ['单股分析（单月统计）', '单股分析（多月统计）', '月榜单', '行业统计', '行业前N支股票'],
    pro:   ['以上全部', '数据校对', '月榜单增强', '行业增强分析', 'Excel导出', 'LOF套利'],
};

async function loadPlanPrices() {
    const container = document.getElementById('plan-price-cards');
    if (!container) return;
    container.innerHTML = '<div class="col-12 text-muted">加载中…</div>';
    try {
        const res = await fetch('/api/admin/plan-prices');
        if (!res.ok) {
            container.innerHTML = `<div class="col-12 text-danger">加载失败（HTTP ${res.status}），请确认已用管理员账号登录且服务器已重启。</div>`;
            return;
        }
        const data = await res.json();
        if (!data.success) {
            container.innerHTML = `<div class="col-12 text-danger">加载失败：${data.detail || data.message || '未知错误'}</div>`;
            return;
        }
        const prices = data.data;
        const borderMap = { basic: 'border-primary', pro: 'border-warning' };
        const colorMap  = { basic: 'text-primary',   pro: 'text-warning'  };
        container.innerHTML = Object.entries(prices).map(([code, plan]) => {
            const isFree     = code === 'free';
            const permsTitle = (PLAN_PERMS[code] || []).join('、');
            const permsCount = (PLAN_PERMS[code] || []).length;
            return `
            <div class="col-md-4">
                <div class="card h-100 ${borderMap[code] || ''}">
                    <div class="card-body p-3">
                        <div class="d-flex align-items-center justify-content-between mb-3">
                            <h6 class="mb-0 fw-semibold ${colorMap[code] || ''}">${plan.name}</h6>
                            <span class="text-muted small" title="${permsTitle}" style="cursor:help;">
                                <i class="bi bi-info-circle"></i> ${permsCount} 项权限
                            </span>
                        </div>
                        ${isFree ? `<div class="text-center text-muted small py-3 border rounded">永久免费，无需配置价格</div>` : `
                        <div class="mb-2">
                            <div class="input-group input-group-sm">
                                <span class="input-group-text text-muted px-2" style="font-size:.75rem;min-width:28px;">月</span>
                                <span class="input-group-text">¥</span>
                                <input type="number" class="form-control" id="price-monthly-${code}"
                                       value="${plan.price_monthly}" min="0" step="0.01" placeholder="月付价格">
                            </div>
                        </div>
                        <div class="mb-2">
                            <div class="input-group input-group-sm">
                                <span class="input-group-text text-muted px-2" style="font-size:.75rem;min-width:28px;">季</span>
                                <span class="input-group-text">¥</span>
                                <input type="number" class="form-control" id="price-quarterly-${code}"
                                       value="${plan.price_quarterly ?? ''}" min="0" step="0.01" placeholder="季付价格">
                            </div>
                        </div>
                        <div>
                            <div class="input-group input-group-sm">
                                <span class="input-group-text text-muted px-2" style="font-size:.75rem;min-width:28px;">年</span>
                                <span class="input-group-text">¥</span>
                                <input type="number" class="form-control" id="price-yearly-${code}"
                                       value="${plan.price_yearly}" min="0" step="0.01" placeholder="年付价格">
                            </div>
                        </div>`}
                    </div>
                </div>
            </div>`;
        }).join('');
    } catch (e) {
        console.error('加载套餐价格失败:', e);
    }
}

async function savePlanPrices() {
    const payload = {};
    for (const code of ['basic', 'pro']) {
        const monthly    = document.getElementById(`price-monthly-${code}`);
        const quarterly  = document.getElementById(`price-quarterly-${code}`);
        const yearly     = document.getElementById(`price-yearly-${code}`);
        if (monthly && yearly) {
            payload[code] = {
                price_monthly:   parseFloat(monthly.value)   || 0,
                price_quarterly: parseFloat(quarterly?.value) || 0,
                price_yearly:    parseFloat(yearly.value)    || 0,
            };
        }
    }
    try {
        const res = await fetch('/api/admin/plan-prices', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
        });
        const data = await res.json();
        if (data.success) {
            alert('套餐价格已保存');
        } else {
            alert('保存失败: ' + (data.detail || data.message || '未知错误'));
        }
    } catch (e) {
        alert('保存失败: ' + e.message);
    }
}

// ===== 临时促销 =====
async function loadPromotions() {
    const container = document.getElementById('promo-cards');
    if (!container) return;
    container.innerHTML = '<div class="col-12 text-muted">加载中…</div>';
    try {
        const res = await fetch('/api/admin/plan-promotions');
        if (!res.ok) { container.innerHTML = `<div class="col-12 text-danger">加载失败 HTTP ${res.status}</div>`; return; }
        const data = await res.json();
        if (!data.success) return;
        const promos = data.data;
        const NAMES = { basic: '基础版', pro: '专业版' };
        container.innerHTML = Object.entries(promos).map(([code, p]) => `
            <div class="col-md-6">
                <div class="card ${p.enabled ? 'border-warning' : ''}">
                    <div class="card-body p-3">
                        <div class="d-flex align-items-center justify-content-between mb-3">
                            <h6 class="mb-0 fw-semibold">${NAMES[code] || code}</h6>
                            <div class="form-check form-switch mb-0">
                                <input class="form-check-input" type="checkbox" id="promo-enabled-${code}" ${p.enabled ? 'checked' : ''}>
                                <label class="form-check-label small text-muted" for="promo-enabled-${code}">启用</label>
                            </div>
                        </div>
                        <div class="mb-2">
                            <input type="text" class="form-control form-control-sm" id="promo-label-${code}"
                                   value="${p.label || '限时特惠'}" placeholder="促销标签文字（如：限时特惠）">
                        </div>
                        <div class="row g-2 mb-2">
                            <div class="col-4">
                                <div class="input-group input-group-sm">
                                    <span class="input-group-text text-muted px-2" style="font-size:.75rem;min-width:28px;">月</span>
                                    <span class="input-group-text">¥</span>
                                    <input type="number" class="form-control" id="promo-monthly-${code}" value="${p.price_monthly}" min="0" placeholder="月付促销">
                                </div>
                            </div>
                            <div class="col-4">
                                <div class="input-group input-group-sm">
                                    <span class="input-group-text text-muted px-2" style="font-size:.75rem;min-width:28px;">季</span>
                                    <span class="input-group-text">¥</span>
                                    <input type="number" class="form-control" id="promo-quarterly-${code}" value="${p.price_quarterly ?? ''}" min="0" placeholder="季付促销">
                                </div>
                            </div>
                            <div class="col-4">
                                <div class="input-group input-group-sm">
                                    <span class="input-group-text text-muted px-2" style="font-size:.75rem;min-width:28px;">年</span>
                                    <span class="input-group-text">¥</span>
                                    <input type="number" class="form-control" id="promo-yearly-${code}" value="${p.price_yearly}" min="0" placeholder="年付促销">
                                </div>
                            </div>
                        </div>
                        <input type="datetime-local" class="form-control form-control-sm" id="promo-end-${code}"
                               value="${p.end_at ? p.end_at.replace(/(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})/, '$1-$2-$3T$4:$5') : ''}"
                               title="结束时间（留空则永久有效）">
                    </div>
                </div>
            </div>`).join('');
    } catch (e) {
        container.innerHTML = `<div class="col-12 text-danger">加载失败: ${e.message}</div>`;
    }
}

async function savePromotions() {
    const payload = {};
    for (const code of ['basic', 'pro']) {
        const enabled   = document.getElementById(`promo-enabled-${code}`)?.checked || false;
        const label     = document.getElementById(`promo-label-${code}`)?.value.trim() || '限时特惠';
        const monthly   = parseFloat(document.getElementById(`promo-monthly-${code}`)?.value)   || 0;
        const quarterly = parseFloat(document.getElementById(`promo-quarterly-${code}`)?.value) || 0;
        const yearly    = parseFloat(document.getElementById(`promo-yearly-${code}`)?.value)    || 0;
        const endRaw    = document.getElementById(`promo-end-${code}`)?.value || '';
        const end_at    = endRaw ? endRaw.replace(/[-T:]/g, '').padEnd(14, '0') : '';
        payload[code]   = { enabled, label, price_monthly: monthly, price_quarterly: quarterly, price_yearly: yearly, end_at };
    }
    try {
        const res = await fetch('/api/admin/plan-promotions', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
        });
        const data = await res.json();
        alert(data.success ? '促销配置已保存' : '保存失败: ' + (data.detail || data.message));
        if (data.success) loadPromotions();
    } catch (e) {
        alert('保存失败: ' + e.message);
    }
}

// ===== 免费试用配置 =====
async function loadTrials() {
    const container = document.getElementById('trial-cards');
    if (!container) return;
    container.innerHTML = '<div class="col-12 text-muted">加载中…</div>';
    try {
        const res = await fetch('/api/admin/plan-trials');
        if (!res.ok) { container.innerHTML = `<div class="col-12 text-danger">加载失败 HTTP ${res.status}</div>`; return; }
        const data = await res.json();
        if (!data.success) { container.innerHTML = `<div class="col-12 text-danger">加载失败：${data.message || '未知错误'}</div>`; return; }
        const trials = data.data;
        const NAMES = { basic: '基础版', pro: '专业版' };
        container.innerHTML = Object.entries(trials).map(([code, t]) => `
            <div class="col-md-6">
                <div class="card ${t.enabled ? 'border-info' : ''}">
                    <div class="card-body p-3">
                        <div class="d-flex align-items-center gap-3">
                            <div class="form-check form-switch mb-0 flex-shrink-0">
                                <input class="form-check-input" type="checkbox" id="trial-enabled-${code}" ${t.enabled ? 'checked' : ''}>
                            </div>
                            <h6 class="mb-0 fw-semibold">${NAMES[code] || code}</h6>
                            <div class="input-group input-group-sm ms-auto" style="max-width:110px;">
                                <input type="number" class="form-control" id="trial-days-${code}" value="${t.days || 7}" min="1" max="365">
                                <span class="input-group-text">天</span>
                            </div>
                        </div>
                    </div>
                </div>
            </div>`).join('');
    } catch (e) {
        container.innerHTML = `<div class="col-12 text-danger">加载失败: ${e.message}</div>`;
    }
}

async function saveTrials() {
    const payload = {};
    for (const code of ['basic', 'pro']) {
        const enabled = document.getElementById(`trial-enabled-${code}`)?.checked || false;
        const days    = parseInt(document.getElementById(`trial-days-${code}`)?.value) || 7;
        payload[code] = { enabled, days };
    }
    try {
        const res = await fetch('/api/admin/plan-trials', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
        });
        const data = await res.json();
        alert(data.success ? '试用配置已保存' : '保存失败: ' + (data.detail || data.message));
        if (data.success) loadTrials();
    } catch (e) {
        alert('保存失败: ' + e.message);
    }
}

// ===== 试用记录 =====
async function loadTrialRecords() {
    const container = document.getElementById('trial-records-container');
    if (!container) return;
    container.innerHTML = '<div class="text-muted text-center py-4">加载中…</div>';
    try {
        const res = await fetch('/api/admin/trials');
        if (!res.ok) { container.innerHTML = `<div class="text-danger text-center py-3">加载失败 HTTP ${res.status}</div>`; return; }
        const data = await res.json();
        if (!data.success) { container.innerHTML = '<div class="text-danger text-center py-3">加载失败</div>'; return; }
        const rows = data.data;
        if (!rows.length) {
            container.innerHTML = '<div class="text-muted text-center py-4">暂无试用记录</div>';
            return;
        }
        const planColors = { basic: '#1677ff', pro: '#fa8c16' };
        const fmtDate = (s) => s ? `${s.slice(0,4)}-${s.slice(4,6)}-${s.slice(6,8)}` : '-';
        const statusBadge = (r) => {
            if (r.status === 'pending')  return `<span class="badge" style="background:#fa8c16;">待审核</span>`;
            if (r.status === 'active')   return `<span class="badge" style="background:#52c41a;">试用中 · 剩${r.days_left}天</span>`;
            if (r.status === 'rejected') return `<span class="badge bg-danger">已拒绝</span>`;
            return `<span class="badge bg-secondary">已到期</span>`;
        };
        const actionBtns = (r) => r.status === 'pending' ? `
            <button class="btn btn-xs btn-success py-0 px-2 me-1" style="font-size:.75rem;" onclick="approveTrial(${r.id}, '${r.plan_name}', ${r.trial_days})">通过</button>
            <button class="btn btn-xs btn-outline-danger py-0 px-2" style="font-size:.75rem;" onclick="rejectTrial(${r.id}, '${r.plan_name}')">拒绝</button>
        ` : '';
        container.innerHTML = `
            <div class="table-responsive">
                <table class="table table-sm table-hover mb-0" style="font-size:.875rem;">
                    <thead class="table-light">
                        <tr>
                            <th>用户名</th>
                            <th>真实姓名</th>
                            <th>手机号</th>
                            <th>身份证</th>
                            <th>套餐</th>
                            <th>申请时间</th>
                            <th>到期时间</th>
                            <th>状态</th>
                            <th>操作</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${rows.map(r => `<tr>
                            <td>${r.username}</td>
                            <td>${r.real_name || '-'}</td>
                            <td>${r.phone || '-'}</td>
                            <td style="font-size:.8rem;color:#666;">${r.id_card || '-'}</td>
                            <td><span style="color:${planColors[r.plan_code]||'#666'};font-weight:600;">${r.plan_name}</span></td>
                            <td>${fmtDate(r.applied_at)}</td>
                            <td>${fmtDate(r.expires_at)}</td>
                            <td>${statusBadge(r)}</td>
                            <td>${actionBtns(r)}</td>
                        </tr>`).join('')}
                    </tbody>
                </table>
            </div>`;
    } catch (e) {
        container.innerHTML = `<div class="text-danger text-center py-3">加载失败: ${e.message}</div>`;
    }
}

async function approveTrial(id, planName, days) {
    if (!confirm(`确认通过「${planName}」试用申请？将开启 ${days} 天试用期。`)) return;
    try {
        const res = await fetch(`/api/admin/trials/${id}/approve`, { method: 'POST' });
        const d = await res.json();
        alert(d.message || (d.success ? '已通过' : '操作失败'));
        if (d.success) loadTrialRecords();
    } catch (e) { alert('操作失败: ' + e.message); }
}

async function rejectTrial(id, planName) {
    const note = prompt(`拒绝「${planName}」试用申请\n\n填写拒绝原因（可选，将展示给用户）：`, '');
    if (note === null) return; // 点取消
    try {
        const res = await fetch(`/api/admin/trials/${id}/reject`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ note }),
        });
        const d = await res.json();
        alert(d.message || (d.success ? '已拒绝' : '操作失败'));
        if (d.success) loadTrialRecords();
    } catch (e) { alert('操作失败: ' + e.message); }
}

// ===== 支付配置（JeePay）=====
async function loadPaymentConfig() {
    try {
        const res = await fetch('/api/admin/payment-config');
        if (!res.ok) { console.error('支付配置加载失败 HTTP', res.status); return; }
        const data = await res.json();
        if (!data.success) return;
        const j = data.data.jeepay || {};
        document.getElementById('jeepay-gateway').value    = j.gateway    || '';
        document.getElementById('jeepay-mch-no').value     = j.mch_no     || '';
        document.getElementById('jeepay-app-id').value     = j.app_id     || '';
        document.getElementById('jeepay-app-secret').value = j.app_secret || '';
        document.getElementById('jeepay-notify-url').value = j.notify_url || '';
    } catch (e) {
        console.error('加载支付配置失败:', e);
    }
}

async function savePaymentConfig() {
    const payload = {
        jeepay: {
            gateway:    document.getElementById('jeepay-gateway').value.trim(),
            mch_no:     document.getElementById('jeepay-mch-no').value.trim(),
            app_id:     document.getElementById('jeepay-app-id').value.trim(),
            app_secret: document.getElementById('jeepay-app-secret').value.trim(),
            notify_url: document.getElementById('jeepay-notify-url').value.trim(),
        },
    };
    try {
        const res = await fetch('/api/admin/payment-config', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
        });
        const data = await res.json();
        data.success ? alert('支付配置已保存') : alert('保存失败: ' + (data.detail || data.message));
    } catch (e) {
        alert('保存失败: ' + e.message);
    }
}

// ========== 我的订阅 ==========

async function loadMySubscription() {
    try {
        const res = await fetch('/api/my/subscription', { credentials: 'include' });
        const d = await res.json();
        if (!d.success) return;
        const sub = d.data;

        // 套餐徽章
        const badge = document.getElementById('my-plan-badge');
        const colors = { free: 'secondary', basic: 'primary', pro: 'warning' };
        const names = { free: '免费版', basic: '基础版', pro: '专业版' };
        if (badge) {
            badge.textContent = names[sub.plan_code] || sub.plan_name;
            badge.className = `badge fs-6 px-3 py-2 bg-${colors[sub.plan_code] || 'secondary'}`;
        }

        // 到期信息
        const expiry = document.getElementById('my-plan-expiry');
        if (expiry) {
            if (sub.valid_until) {
                const y=sub.valid_until.slice(0,4), mo=sub.valid_until.slice(4,6), d2=sub.valid_until.slice(6,8);
                expiry.textContent = `到期：${y}-${mo}-${d2}（剩余 ${sub.days_left} 天）`;
            } else {
                expiry.textContent = sub.plan_code === 'free' ? '永久有效' : '';
            }
        }

        // 到期提醒（剩余7天内）
        const tip = document.getElementById('my-sub-expire-tip');
        const tipText = document.getElementById('my-sub-expire-text');
        if (tip && sub.days_left !== null && sub.days_left <= 7 && sub.days_left > 0) {
            tipText.textContent = `订阅将在 ${sub.days_left} 天后到期，请及时续费`;
            tip.style.display = '';
        }

        // 专业版隐藏升级按钮
        const upgradeBtn = document.getElementById('my-upgrade-btn');
        if (upgradeBtn && sub.plan_code === 'pro') upgradeBtn.style.display = 'none';

    } catch(e) { console.error('loadMySubscription error', e); }

    // 加载订单列表
    loadMyOrders();
    // 加载补偿记录
    loadMyCompensations();
}

async function loadMyOrders() {
    const tbody = document.getElementById('my-orders-tbody');
    if (!tbody) return;
    try {
        const res = await fetch('/api/my/orders', { credentials: 'include' });
        const d = await res.json();
        if (!d.success || !d.data.length) {
            tbody.innerHTML = '<tr><td colspan="6" class="text-center text-muted py-4">暂无订单记录</td></tr>';
            return;
        }
        const statusClass = { '已支付': 'success', '待支付': 'warning', '已过期': 'secondary', '支付失败': 'danger' };
        tbody.innerHTML = d.data.map(o => `
            <tr>
                <td><small class="text-muted">${o.id}</small></td>
                <td>${o.plan_name}</td>
                <td>${o.billing_name}</td>
                <td>¥${o.amount_yuan.toFixed(2)}</td>
                <td><span class="badge bg-${statusClass[o.status_name] || 'secondary'}">${o.status_name}</span></td>
                <td><small>${o.paid_at ? o.paid_at.slice(0,4)+'-'+o.paid_at.slice(4,6)+'-'+o.paid_at.slice(6,8)+' '+o.paid_at.slice(8,10)+':'+o.paid_at.slice(10,12) : '-'}</small></td>
            </tr>`).join('');
    } catch(e) {
        tbody.innerHTML = '<tr><td colspan="6" class="text-center text-danger py-4">加载失败</td></tr>';
    }
}

async function loadMyCompensations() {
    const container = document.getElementById('my-compensations-list');
    if (!container) return;
    try {
        const res = await fetch('/api/my/compensations', { credentials: 'include' });
        const d = await res.json();
        if (!d.success || !d.data.length) {
            container.innerHTML = '<div class="text-center text-muted py-4"><i class="bi bi-check-circle text-success fs-4 d-block mb-2"></i>暂无补偿记录，服务一切正常</div>';
            return;
        }
        container.innerHTML = d.data.map(r => {
            const comp = r.compensated_minutes >= 60
                ? Math.floor(r.compensated_minutes/60) + ' 小时 ' + (r.compensated_minutes % 60 ? r.compensated_minutes % 60 + ' 分钟' : '')
                : r.compensated_minutes + ' 分钟';
            const targetLabel = r.compensation_target === 'trial' ? '试用期' : '订阅';
            const newDate = fmtOutageTime(r.new_valid_until);
            const at = fmtOutageTime(r.created_at);
            return `<div class="d-flex align-items-start gap-3 px-3 py-3 border-bottom">
                <div class="text-success fs-5 pt-1"><i class="bi bi-gift-fill"></i></div>
                <div class="flex-grow-1">
                    <div class="fw-semibold">${r.outage_title}</div>
                    <div class="text-muted small mt-1">
                        已为您的<b>${targetLabel}</b>延长
                        <span class="text-success fw-bold">+ ${comp}</span>，
                        新到期时间：<b>${newDate}</b>
                    </div>
                </div>
                <div class="text-muted small text-nowrap">${at}</div>
            </div>`;
        }).join('');
    } catch(e) {
        container.innerHTML = '<div class="text-center text-danger py-3">加载失败</div>';
    }
}

// ========== 订单管理（管理员）==========

let _orderPage = 1;

async function loadAdminOrders(page) {
    _orderPage = page || 1;
    const tbody = document.getElementById('admin-orders-tbody');
    const statusFilter = document.getElementById('order-filter-status');
    if (!tbody) return;
    const status = statusFilter ? statusFilter.value : '';
    try {
        const url = `/api/admin/orders?page=${_orderPage}&page_size=20${status ? '&status='+status : ''}`;
        const res = await fetch(url, { credentials: 'include' });
        const d = await res.json();
        if (!d.success) return;

        const statusClass = { '已支付': 'success', '待支付': 'warning', '已过期': 'secondary', '支付失败': 'danger' };
        const typeFilter = document.getElementById('order-filter-type') ? document.getElementById('order-filter-type').value : '';
        let rows = d.data;
        if (typeFilter === 'subscription') rows = rows.filter(o => o.plan_code !== 'credits');
        else if (typeFilter === 'credits') rows = rows.filter(o => o.plan_code === 'credits');

        if (!rows.length) {
            tbody.innerHTML = '<tr><td colspan="8" class="text-center text-muted py-4">暂无订单</td></tr>';
        } else {
            tbody.innerHTML = rows.map(o => {
                const isCredit = o.plan_code === 'credits';
                const typeTag = isCredit
                    ? '<span class="badge bg-warning text-dark">点数充值</span>'
                    : '<span class="badge bg-primary">订阅</span>';
                const planDisplay = isCredit ? `点数充值` : o.plan_name;
                const billingDisplay = isCredit ? o.billing_name : o.billing_name;
                return `<tr>
                    <td><small class="text-muted">${o.id}</small></td>
                    <td><strong>${o.username || '-'}</strong></td>
                    <td>${typeTag}</td>
                    <td>${planDisplay}</td>
                    <td>${billingDisplay}</td>
                    <td>¥${o.amount_yuan.toFixed(2)}</td>
                    <td><span class="badge bg-${statusClass[o.status_name] || 'secondary'}">${o.status_name}</span></td>
                    <td><small>${o.paid_at ? o.paid_at.slice(0,4)+'-'+o.paid_at.slice(4,6)+'-'+o.paid_at.slice(6,8)+' '+o.paid_at.slice(8,10)+':'+o.paid_at.slice(10,12) : '-'}</small></td>
                </tr>`;
            }).join('');
        }

        // 分页
        const pagination = document.getElementById('order-pagination');
        if (pagination) {
            const totalPages = Math.ceil(d.total / 20);
            pagination.innerHTML = totalPages <= 1 ? '' : `
                <button class="btn btn-sm btn-outline-secondary" onclick="loadAdminOrders(${_orderPage-1})" ${_orderPage<=1?'disabled':''}>上一页</button>
                <span class="small text-muted">第 ${_orderPage} / ${totalPages} 页，共 ${d.total} 条</span>
                <button class="btn btn-sm btn-outline-secondary" onclick="loadAdminOrders(${_orderPage+1})" ${_orderPage>=totalPages?'disabled':''}>下一页</button>`;
        }
    } catch(e) {
        tbody.innerHTML = '<tr><td colspan="8" class="text-center text-danger py-4">加载失败</td></tr>';
    }
}

// ===== 我的点数 =====
function _renderCreditsBalance(b) {
    const el = document.getElementById('my-credits-balance');
    if (el) el.innerHTML = `
        <span style="font-size:32px;font-weight:800;color:#1677ff;">${b.total}</span>
        <span style="font-size:14px;color:#6b7280;margin-left:4px;">点</span>
        <div style="font-size:12px;color:#9ca3af;margin-top:4px;">
            付费 ${b.balance} 点 · 赠送 ${b.gift_balance} 点
        </div>`;
    updateCreditsDisplay(b.total);
    if (currentUser) { currentUser.credits = b; }
}

function _renderCreditsTx(rows, tbody) {
    if (!tbody) return;
    const typeColors = { recharge: '#1677ff', gift: '#52c41a', deduct: '#f59e0b', expire: '#9ca3af' };
    const typeNames  = { recharge: '充值', gift: '赠送', deduct: '消耗', expire: '过期' };
    tbody.innerHTML = rows.length ? rows.map(t => {
        const isPos = t.type === 'recharge' || t.type === 'gift';
        const dt = t.created_at ? `${t.created_at.slice(0,4)}-${t.created_at.slice(4,6)}-${t.created_at.slice(6,8)} ${t.created_at.slice(8,10)}:${t.created_at.slice(10,12)}` : '-';
        return `<tr>
            <td><span class="badge" style="background:${typeColors[t.type]||'#8c8c8c'}22;color:${typeColors[t.type]||'#8c8c8c'}">${typeNames[t.type]||t.type}</span></td>
            <td style="color:${isPos?'#16a34a':'#dc2626'};font-weight:600;">${isPos?'+':''}${t.credits}</td>
            <td>${t.balance_after}</td>
            <td style="font-size:12px;color:#6b7280;">${t.description||'-'}</td>
            <td style="font-size:12px;color:#9ca3af;">${dt}</td>
        </tr>`;
    }).join('') : '<tr><td colspan="5" class="text-center text-muted py-4">暂无记录</td></tr>';
}

async function loadMyCredits() {
    const tbody = document.getElementById('my-credits-tbody');

    // ① 余额：登录时已随响应返回，直接渲染，零延迟
    if (currentUser && currentUser.credits) {
        _renderCreditsBalance(currentUser.credits);
    }

    // 查询赠送点数审核状态
    fetch('/api/user/gift-status', { credentials: 'include' })
        .then(r => r.json())
        .then(d => {
            const notice = document.getElementById('gift-pending-notice');
            if (!notice) return;
            if (d.success && d.gift_status === 'pending') {
                document.getElementById('gift-pending-amount').textContent = d.gift_amount;
                notice.style.display = '';
            } else {
                notice.style.display = 'none';
            }
        }).catch(() => {});

    // ② 流水：命中预取缓存则直接渲染，感知不到加载
    if (_prefetchCache['credits_tx']) {
        _renderCreditsTx(_prefetchCache['credits_tx'], tbody);
        // 后台静默刷新（不阻塞渲染）
        fetch('/api/credits/transactions?page=1', { credentials: 'include' })
            .then(r => r.json())
            .then(d => { if (d.success) { _prefetchCache['credits_tx'] = d.data; _renderCreditsTx(d.data, tbody); } })
            .catch(() => {});
        return;
    }

    // ③ 缓存未命中（极少数情况）：显示骨架后请求
    if (tbody) tbody.innerHTML = '<tr><td colspan="5" class="text-center text-muted py-3">加载中…</td></tr>';
    try {
        const txRes = await fetch('/api/credits/transactions?page=1', { credentials: 'include' });
        const txD = await txRes.json();
        if (txD.success) {
            _prefetchCache['credits_tx'] = txD.data;
            _renderCreditsTx(txD.data, tbody);
        }
    } catch(e) { console.error('loadMyCredits error', e); }
}

// ===== 管理员手动调整点数 =====
async function adminAdjustCredits(userId, inputId, msgId) {
    const input = document.getElementById(inputId);
    const msg   = document.getElementById(msgId);
    const delta = parseInt(input ? input.value : 0);
    if (!delta || isNaN(delta)) { if(msg) msg.innerHTML='<span class="text-danger">请输入有效数量</span>'; return; }
    const note = `管理员手动${delta > 0 ? '增加' : '扣减'} ${Math.abs(delta)} 点`;
    try {
        const res = await fetch(`/api/admin/users/${userId}/credits`, {
            method: 'POST', credentials: 'include',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ delta, note }),
        });
        const d = await res.json();
        if (msg) msg.innerHTML = d.success
            ? `<span class="text-success">${d.message}，当前余额：${d.balance} 点</span>`
            : `<span class="text-danger">${d.detail || d.message}</span>`;
        if (input) input.value = '';
        if (d.success) loadUsers();
    } catch(e) {
        if (msg) msg.innerHTML = '<span class="text-danger">操作失败</span>';
    }
}

async function manualActivate() {
    const username = document.getElementById('manual-username').value.trim();
    const plan = document.getElementById('manual-plan').value;
    const billing = document.getElementById('manual-billing').value;
    const msg = document.getElementById('manual-activate-msg');
    if (!username) { msg.innerHTML = '<span class="text-danger">请输入用户名</span>'; return; }

    try {
        // 先查用户ID
        const usersRes = await fetch('/api/users', { credentials: 'include' });
        const usersData = await usersRes.json();
        const user = usersData.data && usersData.data.find(u => u.username === username);
        if (!user) { msg.innerHTML = '<span class="text-danger">用户不存在</span>'; return; }

        const res = await fetch(`/api/admin/users/${user.id}/subscription`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'include',
            body: JSON.stringify({ plan_code: plan, billing })
        });
        const d = await res.json();
        if (d.success) {
            const planNames = { basic: '基础版', pro: '专业版' };
            const billingNames = { monthly: '月付', yearly: '年付' };
            msg.innerHTML = `<span class="text-success"><i class="bi bi-check-circle me-1"></i>已为 ${username} 激活 ${planNames[plan]}（${billingNames[billing]}），到期：${d.valid_until ? d.valid_until.slice(0,4)+'-'+d.valid_until.slice(4,6)+'-'+d.valid_until.slice(6,8) : '-'}</span>`;
            loadAdminOrders(_orderPage);
        } else {
            msg.innerHTML = `<span class="text-danger">${d.message || '激活失败'}</span>`;
        }
    } catch(e) {
        msg.innerHTML = '<span class="text-danger">请求失败，请重试</span>';
    }
}

// ========== 收入统计 ==========

let _revenueChart = null;

async function loadRevenueStats() {
    try {
        const res = await fetch('/api/admin/revenue/stats', { credentials: 'include' });
        const d = await res.json();
        if (!d.success) return;
        const s = d.data;

        // 填充指标卡
        document.getElementById('stat-month-revenue').textContent = s.month_revenue.toFixed(2);
        document.getElementById('stat-month-orders').textContent = s.month_orders;
        document.getElementById('stat-active-subs').textContent = s.active_subscribers;
        document.getElementById('stat-total-revenue').textContent = s.total_revenue.toFixed(2);

        // 收入拆分：订阅 vs 充值
        const subRevEl = document.getElementById('stat-sub-revenue');
        const creditRevEl = document.getElementById('stat-credit-revenue');
        if (subRevEl) subRevEl.textContent = s.month_sub_revenue.toFixed(2);
        if (creditRevEl) creditRevEl.textContent = s.month_credit_revenue.toFixed(2);
        const totalSubEl = document.getElementById('stat-total-sub-revenue');
        const totalCreditEl = document.getElementById('stat-total-credit-revenue');
        if (totalSubEl) totalSubEl.textContent = s.total_sub_revenue.toFixed(2);
        if (totalCreditEl) totalCreditEl.textContent = s.total_credit_revenue.toFixed(2);

        // 近30天收入趋势图（用 Canvas 手绘简单折线图，不依赖外部库）
        drawRevenueChart(s.daily);

        // 套餐分布（过滤掉 credits 订单）
        const planNames = { free: '免费版', basic: '基础版', pro: '专业版', credits: '点数充值' };
        const planBody = document.getElementById('plan-dist-body');
        if (planBody) {
            const total = Object.values(s.plan_dist).reduce((a,b)=>a+b, 0) || 1;
            planBody.innerHTML = Object.entries(s.plan_dist).map(([code, cnt]) => `
                <div class="mb-2">
                    <div class="d-flex justify-content-between small mb-1">
                        <span>${planNames[code] || code}</span><span>${cnt} 笔 (${Math.round(cnt/total*100)}%)</span>
                    </div>
                    <div class="progress" style="height:8px;">
                        <div class="progress-bar ${code==='credits'?'bg-warning':''}" style="width:${cnt/total*100}%"></div>
                    </div>
                </div>`).join('') || '<p class="text-muted small">暂无数据</p>';
        }

        // 月付/季付/年付 分布（过滤 credits_ 开头的）
        const billingBody = document.getElementById('billing-dist-body');
        if (billingBody) {
            const billingNames = { monthly: '月付', quarterly: '季付', yearly: '年付' };
            const filtered = Object.entries(s.billing_dist).filter(([k]) => !k.startsWith('credits_'));
            const total2 = filtered.reduce((a,[,v])=>a+v, 0) || 1;
            billingBody.innerHTML = filtered.map(([code, cnt]) => `
                <div class="mb-2">
                    <div class="d-flex justify-content-between small mb-1">
                        <span>${billingNames[code] || code}</span><span>${cnt} 笔 (${Math.round(cnt/total2*100)}%)</span>
                    </div>
                    <div class="progress" style="height:8px;">
                        <div class="progress-bar bg-success" style="width:${cnt/total2*100}%"></div>
                    </div>
                </div>`).join('') || '<p class="text-muted small">暂无数据</p>';
        }
    } catch(e) { console.error('loadRevenueStats error', e); }
}

function drawRevenueChart(daily) {
    const canvas = document.getElementById('revenue-chart');
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    const W = canvas.offsetWidth || 600;
    const H = canvas.offsetHeight || 160;
    canvas.width = W; canvas.height = H;

    const revenues = daily.map(d => d.revenue);
    const maxVal = Math.max(...revenues, 1);
    const pad = { top: 20, right: 20, bottom: 30, left: 50 };
    const chartW = W - pad.left - pad.right;
    const chartH = H - pad.top - pad.bottom;

    ctx.clearRect(0, 0, W, H);
    ctx.strokeStyle = '#e8e8e8';
    ctx.lineWidth = 1;

    // Y轴网格线
    for (let i = 0; i <= 4; i++) {
        const y = pad.top + chartH * (1 - i/4);
        ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(W - pad.right, y); ctx.stroke();
        ctx.fillStyle = '#999'; ctx.font = '11px sans-serif'; ctx.textAlign = 'right';
        ctx.fillText('¥' + Math.round(maxVal * i / 4), pad.left - 4, y + 4);
    }

    // 折线
    ctx.beginPath(); ctx.strokeStyle = '#1677ff'; ctx.lineWidth = 2;
    daily.forEach((d, i) => {
        const x = pad.left + i / (daily.length - 1) * chartW;
        const y = pad.top + chartH * (1 - d.revenue / maxVal);
        i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
    });
    ctx.stroke();

    // 面积填充
    ctx.beginPath();
    daily.forEach((d, i) => {
        const x = pad.left + i / (daily.length - 1) * chartW;
        const y = pad.top + chartH * (1 - d.revenue / maxVal);
        i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
    });
    ctx.lineTo(W - pad.right, pad.top + chartH);
    ctx.lineTo(pad.left, pad.top + chartH);
    ctx.closePath();
    ctx.fillStyle = 'rgba(22,119,255,0.08)'; ctx.fill();

    // X轴日期标签（每5天显示一次）
    ctx.fillStyle = '#999'; ctx.font = '10px sans-serif'; ctx.textAlign = 'center';
    daily.forEach((d, i) => {
        if (i % 5 === 0 || i === daily.length - 1) {
            const x = pad.left + i / (daily.length - 1) * chartW;
            ctx.fillText(d.date, x, H - 4);
        }
    });
}

// ===== 公告管理 =====

const ANN_STYLE_LABELS = { info: '普通', warning: '警告', success: '成功', danger: '重要' };
const ANN_TARGET_LABELS = { all: '所有人', logged_in: '已登录', basic: '基础版+', pro: '专业版' };
const ANN_STYLE_BG = { info: '#1677ff', warning: '#f59e0b', success: '#22c55e', danger: '#ef4444' };
const ANN_STYLE_ICON = { info: 'bi-info-circle-fill', warning: 'bi-exclamation-triangle-fill', success: 'bi-check-circle-fill', danger: 'bi-x-octagon-fill' };

function escHtml(s) {
    return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

// 渲染公告横幅到 #announcement-banners 容器
function renderAnnouncementBanners(list) {
    const container = document.getElementById('announcement-banners');
    if (!container) return;
    container.innerHTML = list.map(a => {
        const bg = ANN_STYLE_BG[a.style] || '#1677ff';
        const icon = ANN_STYLE_ICON[a.style] || 'bi-info-circle-fill';
        return '<div style="background:' + bg + ';color:#fff;padding:7px 16px;font-size:.85rem;display:flex;align-items:center;justify-content:center;gap:8px;" data-ann-id="' + a.id + '">' +
            '<i class="bi ' + icon + '"></i>' +
            '<strong>' + escHtml(a.title) + '</strong>' + (a.content ? '&nbsp;·&nbsp;' + escHtml(a.content) : '') +
            '<span style="margin-left:12px;cursor:pointer;opacity:.7;" onclick="this.closest(\'[data-ann-id]\').remove()"><i class="bi bi-x-lg"></i></span>' +
            '</div>';
    }).join('');
}

// 加载公告横幅（首页登录后调用）
async function loadAnnouncementBanners() {
    try {
        const res = await fetch('/api/announcements');
        const data = await res.json();
        if (data.success) renderAnnouncementBanners(data.data || []);
    } catch(e) {}
}

// 管理员：加载公告列表
async function loadAnnouncementList() {
    const container = document.getElementById('announcement-list-container');
    if (!container) return;
    container.innerHTML = '<div class="text-muted text-center py-4">加载中…</div>';
    try {
        const res = await fetch('/api/admin/announcements');
        const data = await res.json();
        if (!data.success) {
            container.innerHTML = '<div class="text-danger text-center py-4">' + (data.message || '加载失败') + '</div>';
            return;
        }
        const list = data.data || [];
        if (!list.length) {
            container.innerHTML = '<div class="text-muted text-center py-4">暂无公告，点击右上角"新建公告"添加</div>';
            return;
        }
        let rows = list.map(a => {
            const badgeBg = ANN_STYLE_BG[a.style] || '#1677ff';
            const styleLabel = ANN_STYLE_LABELS[a.style] || a.style;
            const targetLabel = ANN_TARGET_LABELS[a.target] || a.target;
            const contentPrev = a.content ? escHtml(a.content.slice(0, 40)) + (a.content.length > 40 ? '…' : '') : '';
            const safeAnn = escHtml(JSON.stringify(a));
            return '<tr>' +
                '<td>' + escHtml(a.title) + (contentPrev ? '<br><small class="text-muted">' + contentPrev + '</small>' : '') + '</td>' +
                '<td><span class="badge" style="background:' + badgeBg + '">' + styleLabel + '</span></td>' +
                '<td>' + targetLabel + '</td>' +
                '<td>' + (a.start_at || '—') + '</td>' +
                '<td>' + (a.end_at || '—') + '</td>' +
                '<td>' + (a.enabled ? '<span class="badge bg-success">启用</span>' : '<span class="badge bg-secondary">停用</span>') + '</td>' +
                '<td>' + a.sort_order + '</td>' +
                '<td>' +
                    '<button class="btn btn-sm btn-outline-primary me-1" onclick="openAnnouncementForm(' + safeAnn + ')">编辑</button>' +
                    '<button class="btn btn-sm btn-outline-danger" onclick="deleteAnnouncement(' + a.id + ')">删除</button>' +
                '</td>' +
                '</tr>';
        }).join('');
        container.innerHTML = '<table class="table table-hover mb-0">' +
            '<thead class="table-light"><tr>' +
            '<th>标题</th><th>样式</th><th>目标用户</th><th>开始时间</th><th>结束时间</th><th>状态</th><th>排序</th><th>操作</th>' +
            '</tr></thead><tbody>' + rows + '</tbody></table>';
    } catch(e) {
        container.innerHTML = '<div class="text-danger text-center py-4">网络错误</div>';
    }
}

// 打开新建/编辑表单（弹窗）
function openAnnouncementForm(ann) {
    const isEdit = !!ann;
    const overlay = document.createElement('div');
    overlay.id = 'ann-modal-overlay';
    overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,.45);z-index:9999;display:flex;align-items:center;justify-content:center;';

    const styleOptions = Object.entries(ANN_STYLE_LABELS).map(function(entry) {
        return '<option value="' + entry[0] + '"' + (isEdit && ann.style === entry[0] ? ' selected' : '') + '>' + entry[1] + '</option>';
    }).join('');
    const targetOptions = Object.entries(ANN_TARGET_LABELS).map(function(entry) {
        return '<option value="' + entry[0] + '"' + (isEdit && ann.target === entry[0] ? ' selected' : '') + '>' + entry[1] + '</option>';
    }).join('');

    const startVal = isEdit && ann.start_at ? ann.start_at.replace(' ', 'T').slice(0, 16) : '';
    const endVal   = isEdit && ann.end_at   ? ann.end_at.replace(' ', 'T').slice(0, 16)   : '';

    overlay.innerHTML =
        '<div style="background:#fff;border-radius:12px;padding:28px;width:520px;max-width:96vw;max-height:90vh;overflow-y:auto;">' +
        '<h5 class="mb-4">' + (isEdit ? '编辑公告' : '新建公告') + '</h5>' +
        '<div class="mb-3"><label class="form-label">标题 <span class="text-danger">*</span></label>' +
        '<input type="text" id="ann-f-title" class="form-control" value="' + (isEdit ? escHtml(ann.title) : '') + '" placeholder="公告标题"></div>' +
        '<div class="mb-3"><label class="form-label">内容（可留空）</label>' +
        '<input type="text" id="ann-f-content" class="form-control" value="' + (isEdit ? escHtml(ann.content || '') : '') + '" placeholder="简短说明（可选）"></div>' +
        '<div class="row g-3 mb-3">' +
        '<div class="col-md-6"><label class="form-label">样式</label><select id="ann-f-style" class="form-select">' + styleOptions + '</select></div>' +
        '<div class="col-md-6"><label class="form-label">目标用户</label><select id="ann-f-target" class="form-select">' + targetOptions + '</select></div>' +
        '</div>' +
        '<div class="row g-3 mb-3">' +
        '<div class="col-md-6"><label class="form-label">开始时间（可留空）</label><input type="datetime-local" id="ann-f-start" class="form-control" value="' + startVal + '"></div>' +
        '<div class="col-md-6"><label class="form-label">结束时间（可留空）</label><input type="datetime-local" id="ann-f-end" class="form-control" value="' + endVal + '"></div>' +
        '</div>' +
        '<div class="row g-3 mb-4">' +
        '<div class="col-md-6"><label class="form-label">排序权重（数值越大越靠前）</label><input type="number" id="ann-f-sort" class="form-control" value="' + (isEdit ? ann.sort_order : 0) + '"></div>' +
        '<div class="col-md-6 d-flex align-items-end"><div class="form-check mb-2">' +
        '<input class="form-check-input" type="checkbox" id="ann-f-enabled"' + ((!isEdit || ann.enabled) ? ' checked' : '') + '>' +
        '<label class="form-check-label" for="ann-f-enabled">立即启用</label></div></div>' +
        '</div>' +
        '<div class="d-flex justify-content-end gap-2">' +
        '<button class="btn btn-secondary" onclick="document.getElementById(\'ann-modal-overlay\').remove()">取消</button>' +
        '<button class="btn btn-primary" onclick="submitAnnouncementForm(' + (isEdit ? ann.id : 'null') + ')">保存</button>' +
        '</div></div>';
    document.body.appendChild(overlay);
}

async function submitAnnouncementForm(annId) {
    const title = document.getElementById('ann-f-title').value.trim();
    if (!title) { alert('标题不能为空'); return; }
    const fmtDt = function(v) { return v ? v.replace('T', ' ') + ':00' : null; };
    const payload = {
        title: title,
        content:    document.getElementById('ann-f-content').value.trim(),
        style:      document.getElementById('ann-f-style').value,
        target:     document.getElementById('ann-f-target').value,
        start_at:   fmtDt(document.getElementById('ann-f-start').value),
        end_at:     fmtDt(document.getElementById('ann-f-end').value),
        sort_order: parseInt(document.getElementById('ann-f-sort').value) || 0,
        enabled:    document.getElementById('ann-f-enabled').checked ? 1 : 0,
    };
    const url    = annId ? '/api/admin/announcements/' + annId : '/api/admin/announcements';
    const method = annId ? 'PUT' : 'POST';
    try {
        const res  = await fetch(url, { method: method, headers: {'Content-Type':'application/json'}, body: JSON.stringify(payload) });
        const data = await res.json();
        if (data.success) {
            document.getElementById('ann-modal-overlay').remove();
            loadAnnouncementList();
        } else {
            alert(data.message || '保存失败');
        }
    } catch(e) {
        alert('网络错误，请稍后重试');
    }
}

async function deleteAnnouncement(annId) {
    if (!confirm('确定删除该公告？')) return;
    try {
        const res  = await fetch('/api/admin/announcements/' + annId, { method: 'DELETE' });
        const data = await res.json();
        if (data.success) loadAnnouncementList();
        else alert(data.message || '删除失败');
    } catch(e) {
        alert('网络错误');
    }
}

// ══════════════════════════════════════════════════
//  数据管理（备份 / 还原）
// ══════════════════════════════════════════════════

// ── Promise 风格确认对话框 ──────────────────────────
function _confirmDialog({ title, body, confirmText = '确认', confirmClass = 'btn-primary', cancelText = '取消' }) {
    return new Promise(resolve => {
        const id  = '_confirm-dlg-' + Date.now();
        const el  = document.createElement('div');
        el.id     = id;
        el.style.cssText = 'position:fixed;inset:0;z-index:99997;display:flex;align-items:center;justify-content:center;background:rgba(0,0,0,.45);';
        el.innerHTML = `
            <div style="background:#fff;border-radius:12px;padding:24px;max-width:460px;width:92%;box-shadow:0 8px 32px rgba(0,0,0,.2);">
                <div style="font-size:15px;font-weight:600;margin-bottom:14px;">${title}</div>
                <div style="font-size:13.5px;color:#374151;margin-bottom:20px;">${body}</div>
                <div style="display:flex;justify-content:flex-end;gap:8px;">
                    <button class="btn btn-outline-secondary btn-sm" id="${id}-cancel">${cancelText}</button>
                    <button class="btn ${confirmClass} btn-sm" id="${id}-ok">${confirmText}</button>
                </div>
            </div>`;
        document.body.appendChild(el);
        const cleanup = (val) => { el.remove(); resolve(val); };
        document.getElementById(`${id}-ok`).onclick     = () => cleanup(true);
        document.getElementById(`${id}-cancel`).onclick = () => cleanup(false);
        el.onclick = (e) => { if (e.target === el) cleanup(false); };
    });
}

// ── 全局底部进度条 ──────────────────────────────────
function _showProgress(label, pct) {
    let el = document.getElementById('_global-progress');
    if (!el) {
        el = document.createElement('div');
        el.id = '_global-progress';
        el.style.cssText = [
            'position:fixed;bottom:0;left:0;right:0;z-index:99998',
            'background:#1a1a2e;color:#fff;padding:10px 18px',
            'box-shadow:0 -2px 16px rgba(0,0,0,.35)',
            'display:flex;align-items:center;gap:14px;font-size:13px',
        ].join(';');
        el.innerHTML = `
            <span id="_gp-label" style="flex-shrink:0;max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;"></span>
            <div style="flex:1;background:rgba(255,255,255,.15);border-radius:4px;height:6px;overflow:hidden;position:relative;">
                <div id="_gp-bar" style="height:100%;background:#4ade80;border-radius:4px;width:0%;transition:width .25s ease;"></div>
            </div>
            <span id="_gp-pct" style="flex-shrink:0;width:38px;text-align:right;font-variant-numeric:tabular-nums;"></span>
        `;
        if (!document.getElementById('_gp-style')) {
            const s = document.createElement('style');
            s.id = '_gp-style';
            s.textContent = '@keyframes _gp-slide{0%{left:-40%;width:40%}60%{left:50%;width:40%}100%{left:110%;width:40%}}';
            document.head.appendChild(s);
        }
        document.body.appendChild(el);
    }
    el.style.display = 'flex';
    document.getElementById('_gp-label').textContent = label;
    const bar = document.getElementById('_gp-bar');
    const pctEl = document.getElementById('_gp-pct');
    if (pct === null) {
        bar.style.cssText = 'position:absolute;height:100%;background:#4ade80;border-radius:4px;width:40%;animation:_gp-slide 1.4s ease infinite;';
        pctEl.textContent = '';
    } else {
        bar.style.cssText = `height:100%;background:#4ade80;border-radius:4px;width:${pct}%;transition:width .25s ease;`;
        pctEl.textContent = pct + '%';
    }
}

function _hideProgress(success = true) {
    const bar = document.getElementById('_gp-bar');
    if (bar) {
        bar.style.cssText = `height:100%;background:${success ? '#4ade80' : '#f87171'};border-radius:4px;width:100%;transition:width .2s ease;`;
        document.getElementById('_gp-pct').textContent = success ? '✓' : '✗';
    }
    setTimeout(() => {
        const el = document.getElementById('_global-progress');
        if (el) el.style.display = 'none';
    }, 900);
}

// XHR 上传 + 进度（返回 Promise<responseData>）
function _xhrUpload(url, formData, labelUpload, labelProcess) {
    return new Promise((resolve, reject) => {
        const xhr = new XMLHttpRequest();
        xhr.open('POST', url);
        xhr.withCredentials = true;
        xhr.upload.onprogress = (e) => {
            if (e.lengthComputable) {
                const pct = Math.round(e.loaded / e.total * 90); // 上传最多到 90%
                _showProgress(labelUpload, pct);
            } else {
                _showProgress(labelUpload, null);
            }
        };
        xhr.upload.onload = () => { _showProgress(labelProcess || labelUpload, null); };
        xhr.onload = () => {
            try { resolve(JSON.parse(xhr.responseText)); }
            catch(e) { reject(new Error('响应解析失败')); }
        };
        xhr.onerror = () => reject(new Error('网络错误'));
        xhr.send(formData);
    });
}

// POST 下载 + 进度（流式读取，自动触发浏览器下载，返回文件名）
async function _postDownload(url, body, label) {
    _showProgress(label, null);
    const res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify(body)
    });
    if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.detail || err.message || 'HTTP ' + res.status);
    }
    const cd = res.headers.get('Content-Disposition') || '';
    const m1 = cd.match(/filename\*=UTF-8''(.+)/);
    const m2 = cd.match(/filename="?([^";\n]+)"?/);
    const filename = m1 ? decodeURIComponent(m1[1]) : m2 ? m2[1] : 'export.xlsx';
    const total = parseInt(res.headers.get('Content-Length') || '0');
    const reader = res.body.getReader();
    const chunks = [];
    let received = 0;
    while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        chunks.push(value);
        received += value.length;
        _showProgress(label, total ? Math.round(received / total * 100) : null);
    }
    const blob = new Blob(chunks);
    const blobUrl = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = blobUrl; a.download = filename;
    document.body.appendChild(a); a.click();
    URL.revokeObjectURL(blobUrl); document.body.removeChild(a);
    _hideProgress(true);
    return filename;
}

// fetch 下载 + 进度（流式读取，自动触发浏览器下载）
async function _fetchDownload(url, label) {
    _showProgress(label, 0);
    const res = await fetch(url, { credentials: 'include' });
    if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.detail || err.message || 'HTTP ' + res.status);
    }
    const cd = res.headers.get('Content-Disposition') || '';
    const m  = cd.match(/filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/);
    const filename = m ? m[1].replace(/['"]/g, '') : 'backup';
    const total = parseInt(res.headers.get('Content-Length') || '0');
    const reader = res.body.getReader();
    const chunks = [];
    let received = 0;
    while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        chunks.push(value);
        received += value.length;
        _showProgress(label, total ? Math.round(received / total * 100) : null);
    }
    const blob = new Blob(chunks);
    const blobUrl = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = blobUrl; a.download = filename;
    document.body.appendChild(a); a.click();
    URL.revokeObjectURL(blobUrl); document.body.removeChild(a);
    _hideProgress(true);
    return filename;
}

function showToast(msg, type) {
    // 轻量 toast 提示（无需 Bootstrap Toast 组件）
    const toast = document.createElement('div');
    const bg = type === 'success' ? '#198754' : type === 'danger' ? '#dc3545' : '#0d6efd';
    toast.style.cssText = 'position:fixed;bottom:24px;right:24px;z-index:99999;background:' + bg +
        ';color:#fff;padding:10px 18px;border-radius:8px;font-size:.9rem;box-shadow:0 4px 16px rgba(0,0,0,.2);max-width:360px;';
    toast.textContent = msg;
    document.body.appendChild(toast);
    setTimeout(function() { toast.style.opacity = '0'; toast.style.transition = 'opacity .4s'; }, 2800);
    setTimeout(function() { toast.remove(); }, 3300);
}

function _fmtSize(bytes) {
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
    return (bytes / 1024 / 1024).toFixed(2) + ' MB';
}

async function loadBackupList() {
    const container = document.getElementById('backup-list-container');
    if (!container) return;
    container.innerHTML = '<div class="text-center text-muted py-3">加载中…</div>';
    try {
        const res  = await fetch('/api/admin/backups');
        const data = await res.json();
        if (!data.success) { container.innerHTML = '<div class="text-danger py-3">' + escHtml(data.message || '加载失败') + '</div>'; return; }
        const list = data.data || [];
        if (!list.length) { container.innerHTML = '<div class="text-center text-muted py-4">暂无备份文件</div>'; return; }

        const BTYPE_LABELS = { user_data: '用户数据', full: '全量', config: '仅配置' };
        const rows = list.map(function(b) {
            return '<tr>' +
                '<td class="text-nowrap">' + escHtml(b.created_at || '—') + '</td>' +
                '<td>' + escHtml(BTYPE_LABELS[b.backup_type] || b.backup_type || '—') + '</td>' +
                '<td>' + _fmtSize(b.size) + '</td>' +
                '<td>' + escHtml(b.app_version || '—') + '</td>' +
                '<td>' + (b.files || []).map(escHtml).join(', ') + '</td>' +
                '<td class="text-nowrap">' +
                    '<a href="/api/admin/backups/' + encodeURIComponent(b.filename) + '/download" class="btn btn-sm btn-outline-primary me-1">下载</a>' +
                    '<button class="btn btn-sm btn-outline-danger" onclick="deleteBackup(\'' + escHtml(b.filename) + '\')">删除</button>' +
                '</td>' +
                '</tr>';
        }).join('');

        container.innerHTML =
            '<table class="table table-hover table-sm mb-0">' +
            '<thead class="table-light"><tr>' +
            '<th>创建时间</th><th>类型</th><th>大小</th><th>版本</th><th>包含文件</th><th>操作</th>' +
            '</tr></thead><tbody>' + rows + '</tbody></table>';
    } catch(e) {
        container.innerHTML = '<div class="text-danger py-3">网络错误</div>';
    }
}

async function createBackup(backupType) {
    const LABELS = { user_data: '用户数据', full: '全量', config: '配置文件' };
    _showProgress('正在创建' + (LABELS[backupType] || '') + '备份…', null);
    try {
        const res  = await fetch('/api/admin/backups', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ backup_type: backupType })
        });
        const data = await res.json();
        if (data.success) {
            _hideProgress(true);
            showToast('备份成功：' + data.filename + '（' + _fmtSize(data.size) + '）', 'success');
            loadBackupList();
        } else {
            _hideProgress(false);
            alert(data.message || '备份失败');
        }
    } catch(e) {
        _hideProgress(false);
        alert('网络错误，请稍后重试');
    }
}

async function deleteBackup(filename) {
    if (!confirm('确定删除备份文件 ' + filename + '？')) return;
    try {
        const res  = await fetch('/api/admin/backups/' + encodeURIComponent(filename), { method: 'DELETE' });
        const data = await res.json();
        if (data.success) { showToast('已删除备份', 'success'); loadBackupList(); }
        else alert(data.message || '删除失败');
    } catch(e) {
        alert('网络错误');
    }
}

async function restoreBackup() {
    const input = document.getElementById('restore-file-input');
    if (!input || !input.files || !input.files.length) { alert('请先选择备份文件'); return; }
    if (!confirm('确定要还原此备份？\n还原前会自动保存一份安全备份，但仍请谨慎操作。')) return;

    const fd = new FormData();
    fd.append('file', input.files[0]);
    try {
        const data = await _xhrUpload('/api/admin/restore', fd, '正在上传备份文件…', '正在还原数据…');
        if (data.success) {
            _hideProgress(true);
            showToast(data.message || '还原成功', 'success');
            loadBackupList();
            input.value = '';
        } else {
            _hideProgress(false);
            alert(data.message || '还原失败');
        }
    } catch(e) {
        _hideProgress(false);
        alert('网络错误，请稍后重试');
    }
}

async function loadBackupConfig() {
    try {
        const res  = await fetch('/api/admin/backup-config');
        const data = await res.json();
        if (!data.success) return;
        const cfg = data.data || {};
        const autoEnable   = document.getElementById('auto-backup-enabled');
        const autoInterval = document.getElementById('auto-backup-interval');
        const autoTime     = document.getElementById('auto-backup-time');
        const autoKeep     = document.getElementById('backup-retention');
        const tipEl        = document.getElementById('backup-config-tip');
        if (autoEnable)   autoEnable.checked = !!cfg.auto_backup_enabled;
        if (autoInterval) autoInterval.value = cfg.auto_backup_interval || 'daily';
        if (autoTime)     autoTime.value     = cfg.auto_backup_time || '02:00';
        if (autoKeep)     autoKeep.value     = cfg.backup_retention || 20;
        if (tipEl && cfg.last_run) tipEl.textContent = '上次自动备份：' + cfg.last_run;
    } catch(e) {
        // 静默失败
    }
}

async function saveBackupConfig() {
    const autoEnable   = document.getElementById('auto-backup-enabled');
    const autoInterval = document.getElementById('auto-backup-interval');
    const autoTime     = document.getElementById('auto-backup-time');
    const autoKeep     = document.getElementById('backup-retention');
    const payload = {
        auto_backup_enabled:  autoEnable   ? autoEnable.checked       : false,
        auto_backup_interval: autoInterval ? autoInterval.value       : 'daily',
        auto_backup_time:     autoTime     ? autoTime.value           : '02:00',
        backup_retention:     autoKeep     ? parseInt(autoKeep.value) : 20,
    };
    try {
        const res  = await fetch('/api/admin/backup-config', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        const data = await res.json();
        if (data.success) showToast('自动备份配置已保存', 'success');
        else alert(data.message || '保存失败');
    } catch(e) {
        alert('网络错误，请稍后重试');
    }
}

// ============================================================
// 宕机补偿管理
// ============================================================

function fmtOutageTime(str) {
    if (!str) return '—';
    // YYYYMMDDHHMMSS → YYYY-MM-DD HH:MM
    if (str.length === 14) {
        return str.slice(0,4)+'-'+str.slice(4,6)+'-'+str.slice(6,8)+' '+str.slice(8,10)+':'+str.slice(10,12);
    }
    return str;
}

const INTERRUPTION_TYPE_LABEL = {
    unplanned: '🔴 意外宕机',
    planned:   '🟡 计划停机',
    degraded:  '🟠 功能降级',
};

const INTERRUPTION_TYPE_DEFAULT_RATIO = {
    unplanned: 1.5,
    planned:   1.0,
    degraded:  1.0,
};

function outageStatusBadge(status) {
    const map = {
        ongoing:     '<span class="badge bg-danger">中断中</span>',
        resolved:    '<span class="badge bg-warning text-dark">待补偿</span>',
        compensated: '<span class="badge bg-success">已补偿</span>',
    };
    return map[status] || '<span class="badge bg-secondary">' + status + '</span>';
}

function onOutageTypeChange() {
    const type = document.getElementById('outage-type').value;
    const ratio = INTERRUPTION_TYPE_DEFAULT_RATIO[type] || 1.5;
    document.getElementById('outage-ratio').value = ratio;
    const hints = {
        unplanned: '意外故障建议补偿 1.5 倍，体现诚意',
        planned:   '计划停机用户有心理预期，1 倍补偿即可',
        degraded:  '功能部分可用，可视影响程度调整倍率',
    };
    document.getElementById('outage-ratio-hint').textContent = hints[type] || '';
}

async function loadOutageList() {
    const container = document.getElementById('outage-list-container');
    container.innerHTML = '<div class="text-muted text-center py-4">加载中…</div>';
    try {
        const res = await fetch('/api/admin/outages');
        const data = await res.json();
        if (!data.success) { container.innerHTML = '<div class="text-danger p-3">加载失败</div>'; return; }
        const list = data.data || [];
        if (!list.length) {
            container.innerHTML = '<div class="text-muted text-center py-4">暂无宕机记录</div>';
            return;
        }
        let html = '<div class="table-responsive"><table class="table table-sm table-hover mb-0">';
        html += '<thead class="table-light"><tr><th>ID</th><th>类型</th><th>事件标题</th><th>开始时间</th><th>结束时间</th><th>时长(分)</th><th>倍率</th><th>状态</th><th>操作</th></tr></thead><tbody>';
        for (const o of list) {
            const safeO = JSON.stringify(o).replace(/'/g, "\\'");
            html += `<tr>
                <td>${o.id}</td>
                <td><small>${INTERRUPTION_TYPE_LABEL[o.interruption_type] || o.interruption_type || '—'}</small></td>
                <td>${o.title}</td>
                <td>${fmtOutageTime(o.started_at)}</td>
                <td>${fmtOutageTime(o.ended_at)}</td>
                <td>${o.duration_minutes ?? '—'}</td>
                <td>${o.compensation_ratio}x</td>
                <td>${outageStatusBadge(o.status)}</td>
                <td class="text-nowrap">`;
            if (o.status === 'ongoing') {
                html += `<button class="btn btn-xs btn-sm btn-warning me-1" onclick="resolveOutage(${o.id})">标记结束</button>`;
            }
            if (o.status === 'resolved') {
                html += `<button class="btn btn-xs btn-sm btn-success me-1" onclick="compensateOutage(${o.id}, '${o.title.replace(/'/g,"\\'")}', ${o.duration_minutes}, ${o.compensation_ratio})">发放补偿</button>`;
            }
            if (o.status === 'compensated') {
                html += `<button class="btn btn-xs btn-sm btn-outline-secondary me-1" onclick="viewOutageRecords(${o.id})">查看明细</button>`;
            }
            html += '</td></tr>';
        }
        html += '</tbody></table></div>';
        container.innerHTML = html;
    } catch(e) {
        container.innerHTML = '<div class="text-danger p-3">网络错误</div>';
    }
}

function openOutageForm() {
    document.getElementById('outage-type').value = 'unplanned';
    document.getElementById('outage-title').value = '';
    document.getElementById('outage-desc').value = '';
    document.getElementById('outage-ratio').value = '1.5';
    document.getElementById('outage-ratio-hint').textContent = '意外故障建议补偿 1.5 倍，体现诚意';
    const now = new Date();
    const pad = n => String(n).padStart(2,'0');
    const local = now.getFullYear()+'-'+pad(now.getMonth()+1)+'-'+pad(now.getDate())+'T'+pad(now.getHours())+':'+pad(now.getMinutes());
    document.getElementById('outage-started').value = local;
    new bootstrap.Modal(document.getElementById('outageModal')).show();
}

async function submitOutage() {
    const title = document.getElementById('outage-title').value.trim();
    if (!title) { alert('请填写事件标题'); return; }
    const desc = document.getElementById('outage-desc').value.trim();
    const ratio = parseFloat(document.getElementById('outage-ratio').value) || 1.5;
    const interruption_type = document.getElementById('outage-type').value;
    let startedRaw = document.getElementById('outage-started').value;
    let started_at = '';
    if (startedRaw) {
        const d = new Date(startedRaw);
        const pad = n => String(n).padStart(2,'0');
        started_at = d.getFullYear()+pad(d.getMonth()+1)+pad(d.getDate())+pad(d.getHours())+pad(d.getMinutes())+'00';
    }
    try {
        const res = await fetch('/api/admin/outages', {
            method: 'POST',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify({ title, description: desc, started_at, compensation_ratio: ratio, interruption_type })
        });
        const data = await res.json();
        if (data.success) {
            bootstrap.Modal.getInstance(document.getElementById('outageModal')).hide();
            showToast('宕机记录已创建', 'success');
            loadOutageList();
        } else {
            alert(data.message || '创建失败');
        }
    } catch(e) {
        alert('网络错误');
    }
}

async function resolveOutage(outageId) {
    if (!confirm('确认标记该宕机事件已结束？结束时间将记录为当前时间。')) return;
    try {
        const res = await fetch('/api/admin/outages/' + outageId + '/resolve', {
            method: 'PUT',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify({})
        });
        const data = await res.json();
        if (data.success) {
            showToast(`已结束，宕机时长 ${data.duration_minutes} 分钟`, 'success');
            loadOutageList();
        } else {
            alert(data.message || '操作失败');
        }
    } catch(e) {
        alert('网络错误');
    }
}

async function compensateOutage(outageId, title, durationMin, ratio) {
    const compMin = Math.round(durationMin * ratio);
    const h = Math.floor(compMin / 60), m = compMin % 60;
    const timeStr = h > 0 ? `${h}小时${m}分钟` : `${m}分钟`;
    if (!confirm(`确认对"${title}"发放补偿？\n\n将对所有有效订阅用户延长 ${timeStr}（${durationMin}分钟 × ${ratio}倍）。\n\n⚠️ 此操作不可撤销！`)) return;
    try {
        const res = await fetch('/api/admin/outages/' + outageId + '/compensate', { method: 'POST' });
        const data = await res.json();
        if (data.success) {
            showToast(data.message, 'success');
            loadOutageList();
        } else {
            alert(data.message || '操作失败');
        }
    } catch(e) {
        alert('网络错误');
    }
}

async function viewOutageRecords(outageId) {
    const container = document.getElementById('outage-records-container');
    container.innerHTML = '加载中…';
    new bootstrap.Modal(document.getElementById('outageRecordsModal')).show();
    try {
        const res = await fetch('/api/admin/outages/' + outageId + '/records');
        const data = await res.json();
        const records = data.data || [];
        if (!records.length) { container.innerHTML = '<div class="text-muted text-center py-3">暂无明细</div>'; return; }
        let html = '<table class="table table-sm table-hover mb-0"><thead class="table-light"><tr><th>用户名</th><th>类型</th><th>补偿时长</th><th>原到期时间</th><th>新到期时间</th><th>发放时间</th></tr></thead><tbody>';
        for (const r of records) {
            const comp = r.compensated_minutes >= 60
                ? Math.floor(r.compensated_minutes/60)+'小时'+r.compensated_minutes%60+'分钟'
                : r.compensated_minutes+'分钟';
            const targetBadge = r.compensation_target === 'trial'
                ? '<span class="badge bg-warning text-dark">试用</span>'
                : '<span class="badge bg-primary">订阅</span>';
            html += `<tr>
                <td>${r.username}</td>
                <td>${targetBadge}</td>
                <td>${comp}</td>
                <td>${fmtOutageTime(r.original_valid_until)}</td>
                <td>${fmtOutageTime(r.new_valid_until)}</td>
                <td>${fmtOutageTime(r.created_at)}</td>
            </tr>`;
        }
        html += '</tbody></table>';
        container.innerHTML = html;
    } catch(e) {
        container.innerHTML = '<div class="text-danger">加载失败</div>';
    }
}


// ========== 工单系统 ==========

const TICKET_TYPE_NAMES = { bug: '功能异常', data: '数据问题', payment: '订阅支付', other: '其他问题' };
const TICKET_STATUS_STYLES = {
    open:       { label: '待处理', color: '#fa541c', bg: '#fff2e8' },
    processing: { label: '处理中', color: '#1677ff', bg: '#e6f4ff' },
    resolved:   { label: '已解决', color: '#52c41a', bg: '#f6ffed' },
};

function showTicketModal() {
    document.getElementById('ticket-type').value = 'bug';
    document.getElementById('ticket-desc').value = '';
    document.getElementById('ticket-submit-err').style.display = 'none';
    const btn = document.getElementById('ticket-submit-btn');
    btn.disabled = false;
    btn.textContent = '提交工单';
    const modal = document.getElementById('ticket-modal');
    modal.style.display = 'flex';
    modal.onclick = e => { if (e.target === modal) modal.style.display = 'none'; };
}

async function submitTicket() {
    const type = document.getElementById('ticket-type').value;
    const desc = document.getElementById('ticket-desc').value.trim();
    const errEl = document.getElementById('ticket-submit-err');
    const btn = document.getElementById('ticket-submit-btn');
    errEl.style.display = 'none';
    if (desc.length < 5) {
        errEl.textContent = '请详细描述问题（至少5个字）';
        errEl.style.display = 'block';
        return;
    }
    btn.disabled = true;
    btn.textContent = '提交中…';
    try {
        const res = await fetch('/api/tickets', {
            method: 'POST', credentials: 'include',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ type, description: desc, page: window.location.pathname }),
        });
        const d = await res.json();
        if (d.success) {
            document.getElementById('ticket-modal').style.display = 'none';
            alert('工单已提交！工单号：#' + d.ticket_id + '\n我们会尽快处理，请在"我的工单"中查看进度。');
            if (document.getElementById('my-tickets') && document.getElementById('my-tickets').style.display !== 'none') {
                loadMyTickets();
            }
        } else {
            errEl.textContent = d.detail || d.message || '提交失败';
            errEl.style.display = 'block';
            btn.disabled = false;
            btn.textContent = '提交工单';
        }
    } catch(e) {
        errEl.textContent = '网络错误，请重试';
        errEl.style.display = 'block';
        btn.disabled = false;
        btn.textContent = '提交工单';
    }
}

async function loadMyTickets() {
    const container = document.getElementById('my-tickets-list');
    if (!container) return;
    container.innerHTML = '<div class="text-center text-muted py-5">加载中…</div>';
    try {
        const res = await fetch('/api/tickets/my', { credentials: 'include' });
        const d = await res.json();
        if (!d.success) {
            container.innerHTML = '<div class="text-danger text-center py-4">加载失败</div>';
            return;
        }
        if (!d.tickets.length) {
            container.innerHTML = '<div class="text-center text-muted py-5"><i class="bi bi-inbox" style="font-size:2rem;"></i><div class="mt-2">暂无工单，遇到问题点右下角"反馈"按钮提交</div></div>';
            return;
        }
        container.innerHTML = d.tickets.map(function(t) {
            const s = TICKET_STATUS_STYLES[t.status] || TICKET_STATUS_STYLES.open;
            const replyHtml = t.reply
                ? '<div style="background:#f6ffed;border-left:3px solid #52c41a;padding:10px 12px;border-radius:0 6px 6px 0;font-size:13px;color:#333;margin-top:10px;">'
                    + '<div style="font-size:11px;color:#52c41a;margin-bottom:4px;font-weight:600;"><i class="bi bi-check-circle me-1"></i>官方回复 · ' + t.replied_at + '</div>'
                    + t.reply + '</div>'
                : '';
            return '<div style="border:1px solid #f0f0f0;border-radius:10px;padding:16px;margin-bottom:12px;background:#fff;">'
                + '<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">'
                + '<div style="display:flex;align-items:center;gap:8px;">'
                + '<span style="font-size:12px;background:#f5f5f5;border-radius:4px;padding:2px 8px;color:#555;">' + t.type_name + '</span>'
                + '<span style="font-size:12px;background:' + s.bg + ';color:' + s.color + ';border-radius:4px;padding:2px 8px;font-weight:600;">' + s.label + '</span>'
                + '<span style="font-size:11px;color:#999;">#' + t.id + '</span>'
                + '</div>'
                + '<span style="font-size:11px;color:#bbb;">' + t.created_at + '</span>'
                + '</div>'
                + '<div style="font-size:13px;color:#333;">' + t.description + '</div>'
                + replyHtml
                + '</div>';
        }).join('');
    } catch(e) {
        container.innerHTML = '<div class="text-danger text-center py-4">加载失败</div>';
    }
}

async function loadAdminTickets(status) {
    const container = document.getElementById('admin-tickets-list');
    if (!container) return;
    container.innerHTML = '<div class="text-center text-muted py-5">加载中…</div>';
    try {
        const url = '/api/admin/tickets' + (status ? ('?status=' + status) : '');
        const res = await fetch(url, { credentials: 'include' });
        const d = await res.json();
        if (!d.success) {
            container.innerHTML = '<div class="text-danger text-center py-4">加载失败</div>';
            return;
        }
        const openCount = d.tickets.filter(function(t) { return t.status === 'open'; }).length;
        const badge = document.getElementById('admin-tickets-badge');
        if (badge) { badge.textContent = openCount || ''; badge.style.display = openCount ? '' : 'none'; }

        if (!d.tickets.length) {
            container.innerHTML = '<div class="text-center text-muted py-5"><i class="bi bi-inbox" style="font-size:2rem;"></i><div class="mt-2">暂无工单</div></div>';
            return;
        }
        const rows = d.tickets.map(function(t) {
            const s = TICKET_STATUS_STYLES[t.status] || TICKET_STATUS_STYLES.open;
            const desc = t.description.length > 40 ? t.description.slice(0, 40) + '…' : t.description;
            const safeDesc = t.description.replace(/'/g, "&#39;").replace(/"/g, "&quot;");
            const safeReply = (t.reply || '').replace(/'/g, "&#39;").replace(/"/g, "&quot;");
            return '<tr>'
                + '<td style="color:#999;">' + t.id + '</td>'
                + '<td><b>' + t.username + '</b></td>'
                + '<td>' + t.type_name + '</td>'
                + '<td style="max-width:200px;" title="' + safeDesc + '">' + desc + '</td>'
                + '<td><span style="background:' + s.bg + ';color:' + s.color + ';border-radius:4px;padding:2px 8px;font-size:12px;font-weight:600;">' + s.label + '</span></td>'
                + '<td style="color:#999;white-space:nowrap;">' + t.created_at + '</td>'
                + '<td><button class="btn btn-sm btn-outline-primary" onclick="showAdminReplyModal(' + t.id + ', \'' + safeDesc + '\', \'' + safeReply + '\', \'' + t.status + '\')">回复</button></td>'
                + '</tr>';
        }).join('');
        container.innerHTML = '<div class="table-responsive"><table class="table table-hover" style="font-size:13px;">'
            + '<thead class="table-light"><tr><th>#</th><th>用户</th><th>类型</th><th>描述</th><th>状态</th><th>提交时间</th><th>操作</th></tr></thead>'
            + '<tbody>' + rows + '</tbody></table></div>';
    } catch(e) {
        container.innerHTML = '<div class="text-danger text-center py-4">加载失败</div>';
    }
}

function showAdminReplyModal(id, description, currentReply, currentStatus) {
    let modal = document.getElementById('admin-reply-modal');
    if (modal) modal.remove();
    modal = document.createElement('div');
    modal.id = 'admin-reply-modal';
    modal.style.cssText = 'position:fixed;inset:0;z-index:9999;display:flex;align-items:center;justify-content:center;background:rgba(0,0,0,0.45);';
    const statusOpts = [
        { v: 'open',       l: '待处理' },
        { v: 'processing', l: '处理中' },
        { v: 'resolved',   l: '已解决' },
    ].map(function(o) {
        return '<option value="' + o.v + '"' + (currentStatus === o.v ? ' selected' : '') + '>' + o.l + '</option>';
    }).join('');
    modal.innerHTML = '<div style="background:#fff;border-radius:12px;width:min(500px,94vw);box-shadow:0 8px 32px rgba(0,0,0,0.18);">'
        + '<div style="padding:16px 20px;border-bottom:1px solid #f0f0f0;display:flex;align-items:center;justify-content:space-between;">'
        + '<strong>回复工单 #' + id + '</strong>'
        + '<button onclick="document.getElementById(\'admin-reply-modal\').remove()" style="border:none;background:none;font-size:20px;color:#999;cursor:pointer;">×</button>'
        + '</div>'
        + '<div style="padding:20px;">'
        + '<div style="background:#f9f9f9;border-radius:6px;padding:10px 12px;font-size:13px;color:#555;margin-bottom:16px;">' + description + '</div>'
        + '<div class="mb-3"><label class="form-label fw-semibold" style="font-size:13px;">回复内容</label>'
        + '<textarea id="admin-reply-text" class="form-control" rows="4" style="font-size:13px;resize:none;">' + currentReply + '</textarea></div>'
        + '<div class="mb-3"><label class="form-label fw-semibold" style="font-size:13px;">更新状态</label>'
        + '<select id="admin-reply-status" class="form-select form-select-sm">' + statusOpts + '</select></div>'
        + '<button onclick="submitAdminReply(' + id + ')" id="admin-reply-btn" style="width:100%;background:#1677ff;color:#fff;border:none;border-radius:7px;padding:10px 0;font-size:14px;font-weight:600;cursor:pointer;">提交回复</button>'
        + '</div></div>';
    modal.addEventListener('click', function(e) { if (e.target === modal) modal.remove(); });
    document.body.appendChild(modal);
}

async function submitAdminReply(id) {
    const reply = document.getElementById('admin-reply-text').value.trim();
    const status = document.getElementById('admin-reply-status').value;
    const btn = document.getElementById('admin-reply-btn');
    btn.disabled = true;
    btn.textContent = '提交中…';
    try {
        const res = await fetch('/api/admin/tickets/' + id, {
            method: 'PUT', credentials: 'include',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ reply: reply, status: status }),
        });
        const d = await res.json();
        if (d.success) {
            document.getElementById('admin-reply-modal').remove();
            loadAdminTickets('');
        } else {
            alert(d.detail || d.message || '提交失败');
            btn.disabled = false;
            btn.textContent = '提交回复';
        }
    } catch(e) {
        alert('网络错误');
        btn.disabled = false;
        btn.textContent = '提交回复';
    }
}

// ========== 注册赠送点数审核 ==========

async function loadPendingGifts() {
    try {
        const res = await fetch('/api/admin/pending-gifts', { credentials: 'include' });
        const d = await res.json();
        if (!d.success) return;
        const card = document.getElementById('pending-gifts-card');
        const list = document.getElementById('pending-gifts-list');
        const badge = document.getElementById('pending-gifts-count');
        if (!card || !list) return;
        const count = d.list.length;
        badge.textContent = count || '';
        if (count === 0) { card.style.display = 'none'; return; }
        card.style.display = '';
        list.innerHTML = d.list.map(function(u) {
            // 计算同IP账号注册间隔
            var accounts = u.ip_accounts || [];
            var accountRows = accounts.map(function(a, idx) {
                var interval = '';
                if (idx > 0) {
                    var prev = new Date(accounts[idx-1].created_at);
                    var curr = new Date(a.created_at);
                    var diff = Math.round((curr - prev) / 1000);
                    if (diff < 60) interval = '<span style="color:#fa541c;font-size:11px;">↑ ' + diff + '秒后</span>';
                    else if (diff < 3600) interval = '<span style="color:#fa8c16;font-size:11px;">↑ ' + Math.round(diff/60) + '分钟后</span>';
                    else if (diff < 86400) interval = '<span style="color:#595959;font-size:11px;">↑ ' + Math.round(diff/3600) + '小时后</span>';
                    else interval = '<span style="color:#595959;font-size:11px;">↑ ' + Math.round(diff/86400) + '天后</span>';
                }
                var isTarget = a.id === u.id;
                var lastLogin = a.last_login
                    ? '<span style="color:#52c41a;">' + a.last_login + '</span>'
                    : '<span style="color:#bbb;">从未登录</span>';
                return '<tr style="' + (isTarget ? 'background:#fffbe6;' : '') + '">'
                    + '<td>' + (isTarget ? '<b>' + a.username + '</b> <span class="badge bg-warning text-dark" style="font-size:10px;">待审</span>' : a.username) + '</td>'
                    + '<td style="color:#999;font-size:12px;">' + a.created_at + ' ' + interval + '</td>'
                    + '<td>' + lastLogin + '</td>'
                    + '</tr>';
            }).join('');

            return '<div style="border:1px solid #ffe58f;border-radius:10px;margin-bottom:16px;overflow:hidden;">'
                + '<div style="background:#fffbe6;padding:12px 16px;display:flex;align-items:center;justify-content:space-between;">'
                + '<div>'
                + '<span style="font-weight:600;font-size:14px;">' + u.username + '</span>'
                + '<span style="margin-left:10px;font-size:12px;color:#999;">IP: ' + u.reg_ip + '</span>'
                + '<span style="margin-left:10px;font-size:12px;color:#595959;">该IP共 <b>' + u.ip_total + '</b> 个账号</span>'
                + '<span class="badge bg-warning text-dark ms-2">' + u.gift_amount + ' 点待审</span>'
                + '</div>'
                + '<div>'
                + '<button class="btn btn-sm btn-success me-2" onclick="handlePendingGift(' + u.id + ',\'approve\')">通过并发放</button>'
                + '<button class="btn btn-sm btn-outline-danger" onclick="handlePendingGift(' + u.id + ',\'reject\')">拒绝</button>'
                + '</div></div>'
                + '<table class="table table-sm mb-0" style="font-size:12px;">'
                + '<thead class="table-light"><tr><th>账号</th><th>注册时间</th><th>最后登录</th></tr></thead>'
                + '<tbody>' + accountRows + '</tbody></table>'
                + '</div>';
        }).join('');
    } catch(e) { console.error('loadPendingGifts error', e); }
}

async function handlePendingGift(userId, action) {
    const label = action === 'approve' ? '通过并发放点数' : '拒绝赠送';
    if (!confirm('确认' + label + '？')) return;
    try {
        const res = await fetch('/api/admin/pending-gifts/' + userId + '/' + action, {
            method: 'POST', credentials: 'include'
        });
        const d = await res.json();
        if (d.success) { loadPendingGifts(); }
        else { alert(d.detail || d.message || '操作失败'); }
    } catch(e) { alert('网络错误'); }
}

// ── 用户修改密码 ──
function showUserChangePwdModal() {
    document.getElementById('user-old-password').value = '';
    document.getElementById('user-new-password').value = '';
    document.getElementById('user-confirm-password').value = '';
    document.getElementById('user-change-pwd-modal').style.display = 'flex';
}
function closeUserChangePwdModal() {
    document.getElementById('user-change-pwd-modal').style.display = 'none';
}
async function submitUserChangePwd() {
    const oldPwd  = document.getElementById('user-old-password').value.trim();
    const newPwd  = document.getElementById('user-new-password').value.trim();
    const confPwd = document.getElementById('user-confirm-password').value.trim();
    if (!oldPwd || !newPwd || !confPwd) { alert('请填写全部字段'); return; }
    if (newPwd !== confPwd) { alert('两次输入的新密码不一致'); return; }
    if (newPwd.length < 6) { alert('新密码至少6位'); return; }
    try {
        const res = await fetch('/api/auth/change-password', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'include',
            body: JSON.stringify({ old_password: oldPwd, new_password: newPwd })
        });
        const d = await res.json();
        if (d.success) {
            closeUserChangePwdModal();
            alert('密码修改成功，请重新登录');
            logout();
        } else {
            alert(d.message || '修改失败');
        }
    } catch(e) { alert('网络错误'); }
}

// ── 忘记密码 ──
function showForgotPwdModal() {
    document.getElementById('forgot-email').value = '';
    document.getElementById('forgot-code').value = '';
    document.getElementById('forgot-newpwd').value = '';
    document.getElementById('forgot-confirmpwd').value = '';
    document.getElementById('forgot-step1-msg').style.display = 'none';
    document.getElementById('forgot-step2-msg').style.display = 'none';
    forgotStep(1);
    document.getElementById('forgot-pwd-modal').style.display = 'flex';
}
function closeForgotPwdModal() {
    document.getElementById('forgot-pwd-modal').style.display = 'none';
}
function forgotStep(n) {
    document.getElementById('forgot-step1').style.display = n === 1 ? 'block' : 'none';
    document.getElementById('forgot-step2').style.display = n === 2 ? 'block' : 'none';
}
async function sendResetCode() {
    const email = document.getElementById('forgot-email').value.trim();
    const msgEl = document.getElementById('forgot-step1-msg');
    if (!email) { showMsg(msgEl, 'danger', '请输入邮箱地址'); return; }
    try {
        const res = await fetch('/api/auth/forgot-password', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ email })
        });
        const d = await res.json();
        if (d.success) {
            forgotStep(2);
        } else {
            showMsg(msgEl, 'danger', d.message || '发送失败');
        }
    } catch(e) { showMsg(msgEl, 'danger', '网络错误'); }
}
async function submitResetPwd() {
    const email    = document.getElementById('forgot-email').value.trim();
    const code     = document.getElementById('forgot-code').value.trim();
    const newPwd   = document.getElementById('forgot-newpwd').value;
    const confPwd  = document.getElementById('forgot-confirmpwd').value;
    const msgEl    = document.getElementById('forgot-step2-msg');
    if (!code) { showMsg(msgEl, 'danger', '请输入验证码'); return; }
    if (!newPwd || newPwd.length < 6) { showMsg(msgEl, 'danger', '新密码至少6位'); return; }
    if (newPwd !== confPwd) { showMsg(msgEl, 'danger', '两次密码不一致'); return; }
    try {
        const res = await fetch('/api/auth/reset-password', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ email, code, new_password: newPwd })
        });
        const d = await res.json();
        if (d.success) {
            closeForgotPwdModal();
            alert('密码重置成功，请重新登录');
        } else {
            showMsg(msgEl, 'danger', d.message || '重置失败');
        }
    } catch(e) { showMsg(msgEl, 'danger', '网络错误'); }
}
function showMsg(el, type, msg) {
    el.className = `alert alert-${type}`;
    el.textContent = msg;
    el.style.display = 'block';
}

// ── SMTP 配置 ──
async function saveSmtpConfig() {
    const smtp = {
        host:      document.getElementById('smtp-host').value.trim(),
        port:      parseInt(document.getElementById('smtp-port').value) || 465,
        use_ssl:   document.getElementById('smtp-ssl').value === 'true',
        user:      document.getElementById('smtp-user').value.trim(),
        password:  document.getElementById('smtp-password').value,
        from_addr: document.getElementById('smtp-from-addr').value.trim(),
    };
    try {
        const res = await fetch('/api/system/config', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'include',
            body: JSON.stringify({ smtp })
        });
        const d = await res.json();
        alert(d.success ? 'SMTP配置已保存' : (d.message || '保存失败'));
    } catch(e) { alert('网络错误'); }
}
async function testSmtpConfig() {
    const to = document.getElementById('smtp-test-email').value.trim();
    if (!to) { alert('请输入测试收件邮箱'); return; }
    try {
        const res = await fetch('/api/auth/test-smtp', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'include',
            body: JSON.stringify({ to })
        });
        const d = await res.json();
        alert(d.message || (d.success ? '发送成功' : '发送失败'));
    } catch(e) { alert('网络错误'); }
}
