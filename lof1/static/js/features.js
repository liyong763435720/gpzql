// ===================================================================
// features.js — 新功能模块：排行榜 / 自定义费率计算器 / 历史走势图
//               设置页 Webhook + 个性化阈值 Tab
// ===================================================================

// ──────────────────────────────────────────────
// 0. 工具：主表格「走势」列注入
// ──────────────────────────────────────────────

/**
 * 在主表格每行末尾（自选星标之前）注入「走势」按钮。
 * 由 app.js 的 displayFunds() 渲染后调用本函数补丁。
 */
function injectHistoryButtons() {
    const tbody = document.getElementById('fundsTableBody');
    if (!tbody) return;
    tbody.querySelectorAll('tr[data-code]').forEach(row => {
        const code = row.getAttribute('data-code');
        // 避免重复注入
        if (row.querySelector('.btn-history')) return;
        // 找到「自选」那列（最后一列），在它之前插入走势列
        const tds = row.querySelectorAll('td');
        const starTd = tds[tds.length - 1];
        const historyTd = document.createElement('td');
        historyTd.innerHTML = `<button class="btn btn-small btn-secondary btn-history" onclick="openHistoryModal('${code}')" title="查看历史走势">走势</button>`;
        row.insertBefore(historyTd, starTd);
    });
}

// 覆盖 app.js 的 displayFunds，在原有逻辑后注入走势按钮
(function patchDisplayFunds() {
    const timer = setInterval(() => {
        if (typeof displayFunds === 'function' && !displayFunds._patched) {
            const _orig = displayFunds;
            window.displayFunds = function(funds) {
                _orig(funds);
                setTimeout(injectHistoryButtons, 50);
            };
            window.displayFunds._patched = true;
            clearInterval(timer);
        }
    }, 100);
})();

// ──────────────────────────────────────────────
// 1. 套利机会排行榜
// ──────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', () => {
    // 排行榜按钮
    const rankingBtn = document.getElementById('rankingBtn');
    if (rankingBtn) rankingBtn.addEventListener('click', openRanking);
    document.getElementById('closeRankingBtn')?.addEventListener('click', closeRanking);
    document.getElementById('closeRankingFooterBtn')?.addEventListener('click', closeRanking);
    document.getElementById('refreshRankingBtn')?.addEventListener('click', loadRanking);

    // 计算器
    document.getElementById('closeCalcBtn')?.addEventListener('click', closeCalc);
    document.getElementById('calcSubmitBtn')?.addEventListener('click', submitCalc);

    // 历史走势图
    document.getElementById('closeHistoryBtn')?.addEventListener('click', closeHistory);
    document.getElementById('closeHistoryFooterBtn')?.addEventListener('click', closeHistory);
    document.querySelectorAll('.history-range-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            document.querySelectorAll('.history-range-btn').forEach(b => b.classList.remove('active'));
            this.classList.add('active');
            loadHistory(_currentHistoryCode, parseInt(this.dataset.days));
        });
    });

    // 设置 Tab 切换
    document.querySelectorAll('.settings-tab-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            document.querySelectorAll('.settings-tab-btn').forEach(b => b.classList.remove('active'));
            document.querySelectorAll('.settings-tab-panel').forEach(p => p.classList.remove('active'));
            this.classList.add('active');
            document.getElementById('settingsTab-' + this.dataset.tab)?.classList.add('active');
        });
    });

    // Webhook 测试
    document.getElementById('testWebhookBtn')?.addEventListener('click', testWebhook);
});

// 打开排行榜
function openRanking() {
    if (typeof checkLogin === 'function' && !checkLogin()) {
        if (typeof requireLogin === 'function') requireLogin();
        return;
    }
    document.getElementById('rankingModal').classList.add('active');
    loadRanking();
}

function closeRanking() {
    document.getElementById('rankingModal').classList.remove('active');
}

async function loadRanking() {
    const tbody = document.getElementById('rankingTableBody');
    tbody.innerHTML = '<tr><td colspan="9" class="loading">加载中...</td></tr>';

    const type = document.getElementById('rankingTypeFilter').value;
    const minRate = parseFloat(document.getElementById('rankingMinRate').value) || 0;

    try {
        const resp = await fetch(`/api/arbitrage/opportunities/ranking?type=${type}&min_rate=${minRate}&limit=100`);
        const data = await resp.json();
        if (!data.success) throw new Error(data.message);

        if (!data.ranking || data.ranking.length === 0) {
            tbody.innerHTML = '<tr><td colspan="9" style="text-align:center;padding:30px;color:#999;">暂无满足条件的套利机会</td></tr>';
            return;
        }

        tbody.innerHTML = data.ranking.map(r => {
            const isFav = typeof favoriteFunds !== 'undefined' && favoriteFunds.has(r.fund_code);
            const typeClass = r.arbitrage_type.includes('溢价') ? 'type-premium' : 'type-discount';
            const diffSign = r.price_diff_pct >= 0 ? '+' : '';
            const limitHtml = (() => {
                const pl = r.purchase_limit || {};
                if (pl.purchase_status === '暂停申购') return '<span style="color:#f44336;">暂停</span>';
                if (pl.purchase_status === '限购' && pl.limit_amount) {
                    const d = pl.limit_amount >= 10000
                        ? (pl.limit_amount / 10000).toFixed(1) + '万'
                        : pl.limit_amount + '元';
                    return `<span style="color:#ff9800;">限${d}</span>`;
                }
                return '<span style="color:#52c41a;">开放</span>';
            })();

            return `
            <tr class="${r.profit_rate > 0 ? 'opportunity' : ''}">
                <td style="text-align:center;font-weight:600;color:${r.rank <= 3 ? '#faad14' : 'inherit'}">
                    ${r.rank <= 3 ? ['★', '★', '★'][r.rank - 1] + ' ' : ''}${r.rank}
                </td>
                <td class="fund-code-name">
                    <strong>${r.fund_code}</strong>${isFav ? ' <span style="color:#faad14;font-size:12px;">★</span>' : ''}<br>
                    <span class="fund-name-text">${r.fund_name || '--'}</span>
                </td>
                <td><span class="arbitrage-type ${typeClass}">${r.arbitrage_type}</span></td>
                <td class="${r.price_diff_pct >= 0 ? 'positive' : 'negative'}">${diffSign}${r.price_diff_pct.toFixed(2)}%</td>
                <td class="profit-rate positive">+${r.profit_rate.toFixed(2)}%</td>
                <td style="color:#1677ff;font-weight:600;">${r.annualized_rate.toFixed(1)}%</td>
                <td style="color:var(--text-secondary);">${r.holding_days}天</td>
                <td>${limitHtml}</td>
                <td style="white-space:nowrap;">
                    <button class="btn btn-small btn-primary"
                        onclick="openCalcModal('${r.fund_code}','${(r.fund_name||'').replace(/'/g,"\\'")}','${r.arbitrage_type}',${r.price},${r.nav})"
                        title="自定义费率计算">计算</button>
                    <button class="btn btn-small btn-secondary"
                        onclick="openHistoryModal('${r.fund_code}')"
                        title="查看历史走势" style="margin-left:4px;">走势</button>
                </td>
            </tr>`;
        }).join('');
    } catch (e) {
        tbody.innerHTML = `<tr><td colspan="9" style="text-align:center;padding:20px;color:#f44336;">加载失败：${e.message}</td></tr>`;
    }
}

// ──────────────────────────────────────────────
// 2. 自定义费率计算器
// ──────────────────────────────────────────────

let _calcFundCode = '';

function openCalcModal(code, name, arbType, price, nav) {
    _calcFundCode = code;
    // 基金信息卡
    const typeClass = arbType.includes('溢价') ? 'type-premium' : 'type-discount';
    const diffPct = nav > 0 ? ((price - nav) / nav * 100).toFixed(2) : '--';
    document.getElementById('calcFundInfo').innerHTML = `
        <div class="calc-fund-card">
            <div class="calc-fund-header">
                <strong>${code}</strong>
                <span style="margin:0 8px;">${name}</span>
                <span class="arbitrage-type ${typeClass}">${arbType}</span>
            </div>
            <div class="calc-fund-prices">
                <span>场内价格 <strong>${price.toFixed(4)}</strong></span>
                <span>场外净值 <strong>${nav.toFixed(4)}</strong></span>
                <span class="${parseFloat(diffPct) >= 0 ? 'positive' : 'negative'}">溢价率 <strong>${parseFloat(diffPct) >= 0 ? '+' : ''}${diffPct}%</strong></span>
            </div>
        </div>`;

    // 清空上次结果
    document.getElementById('calcResult').style.display = 'none';
    document.getElementById('calcSubscribeFee').value = '';
    document.getElementById('calcRedeemFee').value = '';
    document.getElementById('calcBuyComm').value = '';
    document.getElementById('calcSellComm').value = '';

    document.getElementById('calcModal').classList.add('active');
}

function closeCalc() {
    document.getElementById('calcModal').classList.remove('active');
}

async function submitCalc() {
    if (!_calcFundCode) return;
    const amount = parseFloat(document.getElementById('calcAmount').value) || 10000;

    const toFee = id => {
        const v = document.getElementById(id).value;
        return v === '' ? null : parseFloat(v) / 100;
    };
    const fees = {
        subscribe_fee: toFee('calcSubscribeFee'),
        redeem_fee: toFee('calcRedeemFee'),
        buy_commission: toFee('calcBuyComm'),
        sell_commission: toFee('calcSellComm'),
    };
    // 过滤 null
    Object.keys(fees).forEach(k => fees[k] === null && delete fees[k]);

    const btn = document.getElementById('calcSubmitBtn');
    btn.textContent = '计算中...';
    btn.disabled = true;

    try {
        const [calcResp, riskResp] = await Promise.all([
            fetch('/api/arbitrage/calculate', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ fund_code: _calcFundCode, amount, fees }),
            }),
            fetch(`/api/trading-calendar/risk-tips?type=${document.querySelector('#calcFundInfo .arbitrage-type')?.classList.contains('type-premium') ? 'premium' : 'discount'}`),
        ]);

        const calcData = await calcResp.json();
        const riskData = await riskResp.json();

        if (!calcData.success) throw new Error(calcData.message);

        const d = calcData.data;
        const r = d.result;
        const def = d.default_result;

        document.getElementById('calcResultGrid').innerHTML = `
            <div class="calc-compare">
                <div class="calc-col">
                    <div class="calc-col-title">您的费率</div>
                    <div class="calc-metric">
                        <span>净收益</span>
                        <strong class="positive">+¥${r.profit.toFixed(2)}</strong>
                    </div>
                    <div class="calc-metric">
                        <span>收益率</span>
                        <strong class="positive">+${r.profit_rate.toFixed(3)}%</strong>
                    </div>
                    <div class="calc-metric">
                        <span>年化收益</span>
                        <strong style="color:#1677ff;">${r.annualized_rate.toFixed(1)}%</strong>
                    </div>
                    <div class="calc-metric">
                        <span>预计持仓</span>
                        <strong>${r.holding_days} 个交易日</strong>
                    </div>
                    <div class="calc-metric">
                        <span>综合费率</span>
                        <strong>${r.total_cost_rate.toFixed(3)}%</strong>
                    </div>
                </div>
                <div class="calc-divider"></div>
                <div class="calc-col">
                    <div class="calc-col-title">默认费率对比</div>
                    <div class="calc-metric">
                        <span>净收益</span>
                        <strong class="${def.profit >= 0 ? 'positive' : 'negative'}">${def.profit >= 0 ? '+' : ''}¥${def.profit.toFixed(2)}</strong>
                    </div>
                    <div class="calc-metric">
                        <span>收益率</span>
                        <strong class="${def.profit_rate >= 0 ? 'positive' : 'negative'}">${def.profit_rate >= 0 ? '+' : ''}${def.profit_rate.toFixed(3)}%</strong>
                    </div>
                    <div class="calc-metric">
                        <span>年化收益</span>
                        <strong style="color:#1677ff;">${def.annualized_rate.toFixed(1)}%</strong>
                    </div>
                    <div class="calc-metric">
                        <span>费率节省</span>
                        <strong class="${d.fee_saving >= 0 ? 'positive' : 'negative'}">${d.fee_saving >= 0 ? '+' : ''}¥${d.fee_saving.toFixed(2)}</strong>
                    </div>
                    <div class="calc-metric">
                        <span>综合费率</span>
                        <strong>${def.total_cost_rate.toFixed(3)}%</strong>
                    </div>
                </div>
            </div>`;

        // 风控提示
        const riskEl = document.getElementById('calcRiskTips');
        if (riskData.success && riskData.tips) {
            const tips = riskData.tips;
            const lvColor = { low: '#52c41a', medium: '#faad14', high: '#f44336' };
            const lvText = { low: '低风险', medium: '中等风险', high: '注意风险' };
            const color = lvColor[tips.risk_level] || '#666';
            riskEl.innerHTML = `
                <div class="risk-tips-box" style="border-left:3px solid ${color};">
                    <div class="risk-tips-header">
                        <span style="color:${color};font-weight:600;">时间风控提示</span>
                        <span class="risk-badge" style="background:${color};">${lvText[tips.risk_level] || ''}</span>
                    </div>
                    <div class="risk-tips-meta">
                        预计结束日期：<strong>${tips.expected_end_date}</strong> &nbsp;|&nbsp;
                        持仓 <strong>${tips.holding_trading_days}</strong> 个交易日（约 <strong>${tips.holding_calendar_days}</strong> 自然日）
                        ${tips.holiday_count > 0 ? `&nbsp;|&nbsp; <span style="color:${lvColor.medium};">含 ${tips.holiday_count} 天节假日</span>` : ''}
                    </div>
                    <ul class="risk-tips-list">
                        ${tips.risks.map(t => `<li>${t}</li>`).join('')}
                    </ul>
                </div>`;
        } else {
            riskEl.innerHTML = '';
        }

        document.getElementById('calcResult').style.display = 'block';
    } catch (e) {
        alert('计算失败：' + e.message);
    } finally {
        btn.textContent = '计算';
        btn.disabled = false;
    }
}

// ──────────────────────────────────────────────
// 3. 历史走势图（ECharts）
// ──────────────────────────────────────────────

let _historyChart = null;
let _currentHistoryCode = '';
let _currentHistoryDays = 7;

function openHistoryModal(code) {
    _currentHistoryCode = code;
    _currentHistoryDays = 7;
    document.querySelectorAll('.history-range-btn').forEach(b => {
        b.classList.toggle('active', b.dataset.days === '7');
    });
    document.getElementById('historyModal').classList.add('active');
    loadHistory(code, 7);
}

function closeHistory() {
    document.getElementById('historyModal').classList.remove('active');
    if (_historyChart) {
        _historyChart.dispose();
        _historyChart = null;
    }
}

async function loadHistory(code, days) {
    if (!code) return;
    _currentHistoryDays = days;
    const container = document.getElementById('historyChartContainer');
    const noData = document.getElementById('historyNoData');

    // 先销毁旧 chart（canvas 还在 DOM 中），再清空容器，避免 ECharts 对已脱离 DOM 的 canvas 报错
    if (_historyChart) {
        try { _historyChart.dispose(); } catch (_) {}
        _historyChart = null;
    }

    container.style.display = 'block';
    noData.style.display = 'none';
    container.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:100%;color:var(--text-muted);">加载中...</div>';

    try {
        const resp = await fetch(`/api/fund/${code}/history?days=${days}`);
        const data = await resp.json();
        if (!data.success) throw new Error(data.message || '接口返回错误');

        if (!data.history || data.history.length < 2) {
            container.style.display = 'none';
            noData.style.display = 'block';
            document.getElementById('historyModalTitle').textContent = `折溢价率历史走势 — ${data.fund_name || code}`;
            return;
        }

        document.getElementById('historyModalTitle').textContent = `折溢价率历史走势 — ${data.fund_name || code}`;

        const times = data.history.map(h => h.time);
        const diffs = data.history.map(h => h.price_diff_pct);
        const profits = data.history.map(h => h.profit_rate);

        container.innerHTML = ''; // 清空
        if (_historyChart) {
            _historyChart.dispose();
            _historyChart = null;
        }

        _historyChart = echarts.init(container);
        const option = {
            tooltip: {
                trigger: 'axis',
                formatter: params => {
                    let s = `<b>${params[0].axisValue}</b><br/>`;
                    params.forEach(p => {
                        s += `${p.marker}${p.seriesName}：<b>${p.value >= 0 ? '+' : ''}${p.value.toFixed(3)}%</b><br/>`;
                    });
                    return s;
                },
            },
            legend: { data: ['折溢价率', '套利收益率'], top: 0 },
            grid: { left: '3%', right: '4%', bottom: '12%', top: '40px', containLabel: true },
            xAxis: {
                type: 'category',
                data: times,
                axisLabel: { rotate: 30, fontSize: 11, formatter: v => v.slice(5) },
                boundaryGap: false,
            },
            yAxis: {
                type: 'value',
                axisLabel: { formatter: v => v.toFixed(2) + '%' },
                splitLine: { lineStyle: { type: 'dashed' } },
            },
            dataZoom: [{ type: 'inside' }, { type: 'slider', height: 20, bottom: 0 }],
            series: [
                {
                    name: '折溢价率',
                    type: 'line',
                    data: diffs,
                    smooth: true,
                    symbol: 'none',
                    lineStyle: { width: 2 },
                    areaStyle: {
                        color: { type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
                            colorStops: [{ offset: 0, color: 'rgba(22,119,255,.25)' }, { offset: 1, color: 'rgba(22,119,255,0)' }] }
                    },
                    markLine: {
                        silent: true,
                        lineStyle: { type: 'dashed', color: '#52c41a' },
                        data: [{ yAxis: 0.5, name: '阈值 0.5%' }],
                        label: { formatter: '阈值 0.5%', color: '#52c41a' },
                    },
                },
                {
                    name: '套利收益率',
                    type: 'line',
                    data: profits,
                    smooth: true,
                    symbol: 'none',
                    lineStyle: { width: 2, color: '#faad14' },
                },
            ],
        };
        _historyChart.setOption(option);
        // 响应弹窗尺寸变化
        setTimeout(() => _historyChart && _historyChart.resize(), 100);
    } catch (e) {
        container.innerHTML = `<div style="display:flex;align-items:center;justify-content:center;height:100%;color:var(--danger);">加载失败：${e.message}</div>`;
    }
}

// ──────────────────────────────────────────────
// 4. 设置页 — 提醒 Tab：Webhook + 个性化阈值
// ──────────────────────────────────────────────

// 覆盖 app.js 的 openSettings，在打开时额外加载提醒配置
(function patchOpenSettings() {
    const timer = setInterval(() => {
        if (typeof openSettings === 'function' && !openSettings._patched) {
            const _orig = openSettings;
            window.openSettings = function() {
                _orig();
                loadAlertSettings();
            };
            window.openSettings._patched = true;
            clearInterval(timer);
        }
    }, 100);
})();

// 覆盖 saveSettings，追加保存 Webhook 配置
(function patchSaveSettings() {
    const timer = setInterval(() => {
        if (typeof saveSettings === 'function' && !saveSettings._patched) {
            const _orig = saveSettings;
            window.saveSettings = async function() {
                await _orig();
                await saveWebhookSettings();
                await saveAlertThresholds();
            };
            window.saveSettings._patched = true;
            clearInterval(timer);
        }
    }, 100);
})();

async function loadAlertSettings() {
    try {
        const [whResp, thrResp] = await Promise.all([
            fetch('/api/user/webhook'),
            fetch('/api/user/alert-thresholds'),
        ]);
        const whData = await whResp.json();
        const thrData = await thrResp.json();

        // Webhook
        if (whData.success) {
            const wh = whData.webhook || {};
            document.getElementById('webhookType').value = wh.type || 'dingtalk';
            document.getElementById('webhookUrl').value = wh.url || '';
            document.getElementById('webhookEnabled').checked = !!wh.enabled;
        }

        // 个性化阈值：加载自选基金列表后渲染
        const favoritesResp = await fetch('/api/user/favorites');
        const favData = await favoritesResp.json();
        const favorites = favData.success ? (favData.favorites || []) : [];

        const thresholds = thrData.success ? (thrData.thresholds || {}) : {};
        const defaultThreshold = thrData.success ? (thrData.default_threshold || 0.5) : 0.5;
        const container = document.getElementById('alertThresholdsList');

        if (favorites.length === 0) {
            container.innerHTML = '<div style="color:var(--text-muted);text-align:center;padding:16px;">暂无自选基金</div>';
            return;
        }

        container.innerHTML = favorites.map(code => {
            const thr = thresholds[code] !== undefined ? thresholds[code] : '';
            return `
            <div class="alert-threshold-row" data-code="${code}">
                <span class="alert-threshold-code">${code}</span>
                <input type="number" class="alert-threshold-input" data-code="${code}"
                    value="${thr}" step="0.1" min="0"
                    placeholder="默认 ${defaultThreshold}%"
                    title="触发提醒的最小收益率 (%)">
                <span class="alert-threshold-unit">%</span>
                <button class="btn btn-small btn-danger alert-threshold-clear" data-code="${code}" title="恢复默认">×</button>
            </div>`;
        }).join('');

        // 绑定「恢复默认」按钮
        container.querySelectorAll('.alert-threshold-clear').forEach(btn => {
            btn.addEventListener('click', function() {
                const input = container.querySelector(`.alert-threshold-input[data-code="${this.dataset.code}"]`);
                if (input) input.value = '';
            });
        });
    } catch (e) {
        console.warn('加载提醒设置失败:', e);
    }
}

async function saveWebhookSettings() {
    try {
        const type = document.getElementById('webhookType').value;
        const url = document.getElementById('webhookUrl').value.trim();
        const enabled = document.getElementById('webhookEnabled').checked;
        const cooldown = parseInt(document.getElementById('alertCooldown').value) || 60;

        await fetch('/api/user/webhook', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ type, url, enabled, alert_cooldown_minutes: cooldown }),
        });
    } catch (e) {
        console.warn('保存 Webhook 配置失败:', e);
    }
}

async function saveAlertThresholds() {
    try {
        const rows = document.querySelectorAll('.alert-threshold-input');
        const promises = [];
        rows.forEach(input => {
            const code = input.dataset.code;
            const val = input.value.trim();
            if (val !== '') {
                promises.push(
                    fetch(`/api/user/alert-thresholds/${code}`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ threshold: parseFloat(val) }),
                    })
                );
            } else {
                promises.push(
                    fetch(`/api/user/alert-thresholds/${code}`, { method: 'DELETE' })
                );
            }
        });
        await Promise.all(promises);
    } catch (e) {
        console.warn('保存阈值失败:', e);
    }
}

async function testWebhook() {
    const btn = document.getElementById('testWebhookBtn');
    btn.textContent = '发送中...';
    btn.disabled = true;
    try {
        // 先保存当前配置，再测试
        await saveWebhookSettings();
        const resp = await fetch('/api/user/webhook/test', { method: 'POST' });
        const data = await resp.json();
        if (data.success) {
            btn.textContent = '发送成功!';
            btn.style.color = '#52c41a';
            setTimeout(() => { btn.textContent = '发送测试消息'; btn.style.color = ''; btn.disabled = false; }, 2500);
        } else {
            alert('发送失败：' + data.message);
            btn.textContent = '发送测试消息';
            btn.disabled = false;
        }
    } catch (e) {
        alert('发送失败：' + e.message);
        btn.textContent = '发送测试消息';
        btn.disabled = false;
    }
}
